"""Progressive hybrid retrieval, RRF fusion, and canonical evidence hydration."""
import math
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from .common import required_text, timestamp
from .relevance import RelevanceGate
from .trace import LOG, debug_enabled


def _epoch(value):
    """Parse an ISO occurred_at into epoch seconds; None when absent or unparseable."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


# Small, fast, MS MARCO-tuned cross-encoder used by the optional relevance re-ranker.
# Override via config {"rerank": {"model": "..."}}.
RERANK_DEFAULT_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


def _rerank_disabled_by_env():
    """Operational kill-switch: PERSONAL_MEMORY_DISABLE_RERANK forces the (default-on)
    learned re-ranker off without touching config - used for dependency-light test runs
    and to run a GPU model-free instance; production defaults to enabled."""
    return os.environ.get("PERSONAL_MEMORY_DISABLE_RERANK", "").strip().lower() in {"1", "true", "yes", "on"}


def _semantic_disabled_by_env():
    """Operational kill-switch: PERSONAL_MEMORY_DISABLE_SEMANTIC forces the (default-on) local
    embedding index off without touching config - used for dependency-light test runs and to run
    an instance without the embedding model; production defaults to enabled."""
    return os.environ.get("PERSONAL_MEMORY_DISABLE_SEMANTIC", "").strip().lower() in {"1", "true", "yes", "on"}


class Hybrid:
    def __init__(self, store, config=None, semantic=None, hindsight=None, start=True):
        self.store, self.config = store, config or {}
        self.relevance = RelevanceGate(self.config.get("relevance"))
        # Temporal consolidation: an additive recency bonus on top of RRF so a current fact
        # outranks a lexically-similar but superseded one. ENABLED by default (weight 1.0) as
        # the standard behaviour; a deployment opts out with {"temporal":{"weight":0}}.
        temporal = self.config.get("temporal") or {}
        try:
            self.temporal_weight = float(temporal.get("weight", 1.0))
        except (TypeError, ValueError):
            self.temporal_weight = 1.0
        try:
            self.temporal_half_life = float(temporal.get("half_life_days", 365))
        except (TypeError, ValueError):
            self.temporal_half_life = 365.0
        if not math.isfinite(self.temporal_weight) or self.temporal_weight < 0:
            self.temporal_weight = 1.0
        if not math.isfinite(self.temporal_half_life) or self.temporal_half_life <= 0:
            self.temporal_half_life = 365.0
        self.semantic, self.hindsight = semantic, hindsight
        self.errors, self.threads = {}, []
        self.stop = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="memory-search")
        # Local embedding index: ENABLED by default as the standard behaviour (a deployment opts
        # out with {"semantic":{"enabled":false}} or PERSONAL_MEMORY_DISABLE_SEMANTIC=1). It is
        # built LAZILY on the first search - never at construction - so startup stays cheap and
        # offline, and any import/model failure is permanent and silent, degrading to the
        # keyword+Hindsight channels. This is a relevance path that does not depend on the
        # external engine returning similarity scores.
        sem_cfg = self.config.get("semantic", {})
        self._semantic_cfg = sem_cfg if isinstance(sem_cfg, dict) else {}
        self._semantic_enabled = self.semantic is not None or (
            bool(self._semantic_cfg.get("enabled", True)) and not _semantic_disabled_by_env())
        self._semantic_failed = False
        # The external Hindsight engine stays opt-in: it needs an operator URL/bank, so it is
        # constructed eagerly only when explicitly configured.
        hindsight_cfg = self.config.get("hindsight", {})
        if hindsight_cfg.get("enabled") and self.hindsight is None:
            try:
                from .hindsight import Hindsight
                self.hindsight = Hindsight(store, hindsight_cfg)
            except Exception as error:
                self.errors["hindsight"] = type(error).__name__ + ": initialization failed; check model/dependencies/configuration"
        # Learned re-ranking of the fused top-k. ENABLED by default as the standard behaviour;
        # a deployment opts out with {"rerank":{"enabled":false}}. The model is built LAZILY on
        # the first search (never at construction) so startup stays cheap and offline, and any
        # import/model/scoring failure is permanent and silent - the fusion order stands. This
        # keeps the feature dependency-free to import and safe where the model is unavailable.
        rerank = self.config.get("rerank") or {}
        if not isinstance(rerank, dict):
            rerank = {}
        self.reranker = None
        self._rerank_enabled = bool(rerank.get("enabled", True)) and not _rerank_disabled_by_env()
        self._rerank_failed = False
        self.rerank_model = rerank.get("model", RERANK_DEFAULT_MODEL)
        self.rerank_options = {k: rerank[k] for k in ("device", "max_length") if k in rerank}
        try:
            self.rerank_window = max(2, min(50, int(rerank.get("window", 24))))
        except (TypeError, ValueError):
            self.rerank_window = 24
        # HippoRAG-style multi-hop activation: personalised PageRank over the record<->entity
        # graph (entity_links) so a fact reachable only through an intermediate bridge - sharing
        # no entity with the query's direct hits - can still surface. ENABLED by default as the
        # standard behaviour; a deployment opts out with {"graph":{"enabled":false}}. It is pure,
        # bounded SQL (no external model), so it always degrades gracefully to the direct hits.
        graph = self.config.get("graph") or {}
        if not isinstance(graph, dict):
            graph = {}
        self.graph_enabled = bool(graph.get("enabled", True))
        self.graph_hops = self._bounded_int(graph.get("hops"), 2, 1, 3)
        self.graph_damping = self._bounded_float(graph.get("damping"), 0.6, 0.0, 0.95)
        self.graph_weight = self._bounded_float(graph.get("weight"), 0.3, 0.0, 4.0)
        self.graph_seed_top = self._bounded_int(graph.get("seed_top"), 6, 1, 16)
        self.graph_record_degree = self._bounded_int(graph.get("record_degree"), 6, 1, 16)
        self.graph_entity_degree = self._bounded_int(graph.get("entity_degree"), 16, 1, 64)
        self._start = start
        if start:
            for component in (self.semantic, self.hindsight):
                if component:
                    self._start_index_thread(component)

    def _start_index_thread(self, engine):
        """Run an engine's incremental indexing journal off the request path."""
        def work():
            delay = 0
            while not self.stop.wait(delay):
                try:
                    # Both indexes commit batches transactionally. Draining several records per
                    # wake avoids one HTTP/LLM setup per Hindsight record and one SQLite
                    # transaction per local embedding.
                    delay = 0.05 if engine.sync(batch=8) else 2
                except Exception as error:
                    engine.last_error = type(error).__name__ + ": indexing failed; retry pending"
                    delay = 10
        thread = threading.Thread(target=work, daemon=True, name="memory-index")
        thread.start(); self.threads.append(thread)

    def _ensure_semantic(self):
        """Build the local embedding index on first use. No-op when disabled/injected/failed.

        Any import/model failure is recorded once and made permanent (never retried per query) so
        retrieval degrades to the keyword+Hindsight channels and startup never blocks on the
        embedding model.
        """
        if self.semantic is not None or not self._semantic_enabled or self._semantic_failed:
            return self.semantic
        try:
            from .semantic import SemanticIndex
            engine = SemanticIndex(self.store, self._semantic_cfg)
        except Exception as error:
            self.semantic = None
            self._semantic_failed = True
            self.errors["semantic"] = type(error).__name__ + ": semantic index unavailable; using keyword/hindsight"
            return None
        self.semantic = engine
        if self._start:
            self._start_index_thread(engine)
        return self.semantic

    def close(self):
        self.stop.set()
        self.pool.shutdown(wait=True, cancel_futures=True)
        deadline=time.monotonic()+125
        for thread in self.threads:
            thread.join(timeout=max(0,deadline-time.monotonic()))

    def warmup(self):
        """Prime the lazily-loaded retrieval models so the first real query is fast.

        The embedder (ONNX session), the cross-encoder re-ranker and the external Hindsight
        recall each pay a one-time model/session load on first use - seconds on a GPU - which
        otherwise lands on the first Hermes turn after a restart (observed ~7.7s). Running them
        once at startup, off the request path, moves that cost out of the first user query. This
        is strictly best-effort: it never raises and never writes to ``self.errors`` (which feeds
        readiness), so a warmup failure just means the first query warms the cache instead.
        """
        probe = "warmup"
        self._ensure_semantic()
        if self.semantic is not None:
            try:
                self.semantic.embedder.query(probe)
            except Exception:
                pass
        if self.hindsight is not None:
            try:
                self.hindsight.candidates(probe, "fast")
            except Exception:
                pass
        if self._rerank_enabled:
            try:
                self._ensure_reranker()
            except Exception:
                pass
        return {"semantic": self.semantic is not None, "hindsight": self.hindsight is not None,
                "rerank_loaded": self.reranker is not None}

    def clear_external(self):
        """Cascade a canonical reset to the external retrieval engine so a fresh start leaves no
        residual evidence in the managed Hindsight bank (memories, entities and retained
        documents). No-op when no external engine is bound."""
        if self.hindsight is not None and callable(getattr(self.hindsight, "clear_bank", None)):
            return self.hindsight.clear_bank()
        return {"cleared": False, "reason": "no external engine bound"}

    def status(self):
        result = self.store.status()
        if self.semantic:
            sem_status = self.semantic.status()
        elif self._semantic_enabled and not self._semantic_failed:
            sem_status = {"enabled": True, "ready": False, "lazy": True, "note": "built on first search"}
        else:
            sem_status = {"enabled": False}
        result.update(backend="hybrid_rrf", semantic_embeddings=self.semantic is not None,
                      semantic=sem_status,
                      hindsight=self.hindsight.status() if self.hindsight else {"enabled":False},
                      initialization_errors=self.errors,
                      capabilities=["keyword","temporal_filters","account_identity","evidence","pagination","query_variants"])
        if self._semantic_enabled and not self._semantic_failed:
            result["capabilities"].append("semantic")
        if self.hindsight:
            result["capabilities"].append("hindsight_multistrategy")
        if self._rerank_enabled:
            result["capabilities"].append("cross_encoder_rerank")
            result["rerank"] = {"enabled": True, "model": self.rerank_model, "window": self.rerank_window,
                                "loaded": self.reranker is not None, "unavailable": self._rerank_failed}
        if self.graph_enabled:
            result["capabilities"].append("graph_multihop")
            result["graph"] = {"enabled": True, "hops": self.graph_hops, "damping": self.graph_damping,
                               "weight": round(self.graph_weight, 4), "seed_top": self.graph_seed_top,
                               "record_degree": self.graph_record_degree, "entity_degree": self.graph_entity_degree}
        return result

    @staticmethod
    def _bounded_int(value, default, low, high):
        """Coerce an optional config number to a clamped int, falling back to the default."""
        try:
            return max(low, min(high, int(value)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _bounded_float(value, default, low, high):
        """Coerce an optional config number to a clamped finite float, falling back to the default."""
        try:
            result = float(value)
        except (TypeError, ValueError):
            return default
        return result if math.isfinite(result) else default

    def _propagate_graph(self, seeds, allowed):
        """Bounded personalised PageRank over the record<->entity graph (HippoRAG-style).

        ``seeds`` maps fused top-k record ids to their score; relevance diffuses along shared
        entities so a fact linked only through an intermediate bridge (a different entity, or a
        record that connects two entities) surfaces even when it shares no entity with a direct
        hit. The walk is confined to a local subgraph: per-node degree caps stop a hub entity
        flooding results, ``graph_hops`` bounds diffusion, damping teleports back to the seeds so
        they stay dominant, and ties break on id for determinism. Propagated records are *leads*
        - they still have to clear the relevance gate on hydration. Pure SQL with no external
        dependency; returns [] (never raises) when the store carries no entity structure.
        """
        if not self.graph_enabled or not seeds:
            return []
        record_entities, entity_records = {}, {}
        with self.store.connect() as db:
            def entities_of(rid):
                cached = record_entities.get(rid)
                if cached is None:
                    cached = [row["entity_id"] for row in db.execute(
                        "SELECT DISTINCT entity_id FROM entity_links WHERE record_id=? LIMIT ?",
                        (rid, self.graph_record_degree))]
                    record_entities[rid] = cached
                return cached

            def records_of(eid):
                cached = entity_records.get(eid)
                if cached is None:
                    cached = [row["record_id"] for row in db.execute(
                        """SELECT l.record_id FROM entity_links l JOIN records r ON r.id=l.record_id
                           WHERE l.entity_id=? AND r.deleted=0
                           ORDER BY r.occurred_at DESC LIMIT ?""", (eid, self.graph_entity_degree))]
                    entity_records[eid] = cached
                return cached

            visited = set(seeds)
            frontier = set(seeds)
            for _ in range(self.graph_hops):
                if not frontier:
                    break
                entities = set()
                for rid in frontier:
                    entities.update(entities_of(rid))
                nxt = set()
                for eid in entities:
                    for rid in records_of(eid):
                        if allowed is not None and rid not in allowed:
                            continue
                        if rid not in visited:
                            visited.add(rid); nxt.add(rid)
                frontier = nxt
            for rid in list(visited):
                entities_of(rid)
            for eid in list(entity_records):
                records_of(eid)

        total = sum(max(0.0, s) for s in seeds.values()) or 1.0
        personalization = {rid: max(0.0, score) / total for rid, score in seeds.items()}
        mass = dict(personalization)
        alpha = self.graph_damping
        for _ in range(self.graph_hops):
            entity_mass = defaultdict(float)
            for rid, value in mass.items():
                if value <= 0:
                    continue
                entities = record_entities.get(rid) or []
                if not entities:
                    continue  # dangling mass is re-teleported to the seeds below
                share = value / len(entities)
                for eid in entities:
                    entity_mass[eid] += share
            record_mass = defaultdict(float)
            for eid, value in entity_mass.items():
                if value <= 0:
                    continue
                reachable = [rid for rid in (entity_records.get(eid) or []) if rid in visited]
                if not reachable:
                    continue
                share = value / len(reachable)
                for rid in reachable:
                    record_mass[rid] += share
            mass = {rid: (1 - alpha) * personalization.get(rid, 0.0) + alpha * record_mass.get(rid, 0.0)
                    for rid in visited}

        propagated = []
        for rid in visited:
            if rid in seeds:
                continue
            score = mass.get(rid, 0.0)
            if score <= 1e-9:
                continue
            via, origin = None, None
            for eid in record_entities.get(rid) or []:
                for src in entity_records.get(eid) or []:
                    if src in seeds:
                        via, origin = eid, src
                        break
                if via:
                    break
            propagated.append({"id": rid, "score": score, "via_entity": via, "from_record": origin})
        propagated.sort(key=lambda item: (-item["score"], item["id"]))
        return propagated

    def _apply_recency(self, ordered, scores):
        """Re-rank already-retrieved candidates with a bounded exponential recency bonus.

        Rank-based fusion carries no notion of time, so a stale fact can tie or edge a
        current one. The bonus is multiplicative on the RRF score and capped at
        (1 + temporal_weight), demoting nothing and only ever preferring fresher, already
        relevant evidence. Unknown or undated records keep their fusion rank.
        """
        if not ordered or self.temporal_weight <= 0:
            return ordered
        now = time.time()
        half = self.temporal_half_life
        ids = list(ordered)
        times = {}
        with self.store.connect() as db:
            for start in range(0, len(ids), 500):
                chunk = ids[start:start + 500]
                marks = ",".join("?" * len(chunk))
                for row in db.execute("SELECT id, occurred_at FROM records WHERE id IN (" + marks + ")", chunk):
                    times[row["id"]] = row["occurred_at"]

        def adjusted(rid):
            stamp = _epoch(times.get(rid))
            if stamp is None:
                return scores[rid]
            age_days = max(0.0, (now - stamp) / 86400.0)
            return scores[rid] * (1.0 + self.temporal_weight * (0.5 ** (age_days / half)))

        return sorted(ordered, key=lambda rid: (-adjusted(rid), rid))

    def _ensure_reranker(self):
        """Build the cross-encoder on first use. No-op when disabled or already resolved.

        Any import/model failure is recorded once and made permanent (we do not retry per
        query) so the fused ordering is preserved and startup never blocks on the model.
        """
        if not self._rerank_enabled or self._rerank_failed or self.reranker is not None:
            return self.reranker
        try:
            from sentence_transformers import CrossEncoder
            self.reranker = CrossEncoder(self.rerank_model, **self.rerank_options)
        except Exception as error:
            self.reranker = None
            self._rerank_failed = True
            self.errors["rerank"] = type(error).__name__ + ": reranker unavailable; using fusion order"
        return self.reranker

    def _apply_rerank(self, query, ordered, scores):
        """Reorder the fused top-k with a learned cross-encoder (query, passage) score.

        Reciprocal-rank fusion reflects channel agreement and term statistics, not true
        answer relevance, so a lexically-similar distractor can outrank the passage that
        actually answers. Only the top ``rerank_window`` candidates are rescored; the tail
        keeps its fusion order. This reorders and re-scores existing candidates only - it
        never adds or drops evidence - and is a no-op when explicitly disabled, when the
        model cannot be loaded, or on any scoring failure (graceful degradation to fusion).
        """
        if len(ordered) < 2:
            return ordered, scores
        reranker = self.reranker or self._ensure_reranker()
        if reranker is None:
            return ordered, scores
        window = list(ordered[:self.rerank_window])
        texts = {}
        with self.store.connect() as db:
            for start in range(0, len(window), 200):
                chunk = window[start:start + 200]
                marks = ",".join("?" * len(chunk))
                for row in db.execute("SELECT id, text FROM records WHERE id IN (" + marks + ")", chunk):
                    texts[row["id"]] = row["text"] or ""
        pairs = [(query, texts.get(rid, "")) for rid in window]
        try:
            logits = reranker.predict(pairs, convert_to_numpy=True, show_progress_bar=False)
        except Exception as error:
            self.errors["rerank"] = type(error).__name__ + ": reranking failed; using fusion order"
            return ordered, scores
        values = [float(x) for x in logits]
        low, high = min(values), max(values)
        span = (high - low) or 1.0
        # Map into a [1, 2] band: reranked candidates stay above any fusion-only tail and
        # keep a scale the downstream multiplicative recency bonus can combine with safely.
        for rid, value in zip(window, values):
            scores[rid] = 1.0 + (value - low) / span
        chosen = set(window)
        ranked = sorted(window, key=lambda rid: (-scores[rid], rid))
        tail = [rid for rid in ordered if rid not in chosen]
        return ranked + tail, scores

    def search(self, query, limit=8, entity_id=None, source=None, after=None, before=None,
               include_history=False, depth="balanced", queries=None, expand_entities=True,
               exclude_record_ids=None):
        required_text(query,"query",4000)
        if type(limit) is not int or not 1 <= limit <= 30:
            raise ValueError("limit must be 1..30; use browse cursors for complete stored evidence")
        if depth not in {"fast","balanced","deep"}:
            raise ValueError("depth must be fast, balanced or deep")
        if queries is not None and (not isinstance(queries,list) or len(queries)>4):
            raise ValueError("queries must contain at most four alternative or subquestion queries")
        variants = list(dict.fromkeys([query]+[required_text(q,"query variant",4000) for q in (queries or [])]))
        if exclude_record_ids is None: exclude_record_ids=[]
        if (not isinstance(exclude_record_ids,list) or len(exclude_record_ids)>100 or
                any(not isinstance(rid,str) or not rid for rid in exclude_record_ids)):
            raise ValueError("exclude_record_ids must contain at most 100 record IDs")
        excluded=set(exclude_record_ids)
        if after: after=timestamp(after)
        if before: before=timestamp(before)
        if after and before and before<=after:
            raise ValueError("before must follow after")
        started = time.monotonic()
        allowed = self.store.related_ids(entity_id) if entity_id else None
        clauses, params = ["deleted=0"], []
        if source: clauses.append("source=?"); params.append(source)
        if after: clauses.append("occurred_at>=?"); params.append(after)
        if before: clauses.append("occurred_at<?"); params.append(before)
        if after or before: clauses.append("occurred_at!=''")
        if source or after or before:
            with self.store.connect() as db:
                live = {r[0] for r in db.execute("SELECT id FROM records WHERE " + " AND ".join(clauses), params)}
            allowed = live if allowed is None else allowed & live
        pool_size = {"fast":32,"balanced":96,"deep":256}[depth]
        engines = {}
        scores, reasons, spans, claim_rows = defaultdict(float), defaultdict(set), {}, {}
        failures, counts = dict(self.errors), {}
        similarities = {}; rejected = 0

        def add(channel, rows, weight=1):
            seen=set()
            for rank,row in enumerate(rows,1):
                rid=row["id"]
                if rid in excluded or (allowed is not None and rid not in allowed) or rid in seen: continue
                seen.add(rid)
                if "similarity" in row:
                    similarities[rid] = max(similarities.get(rid, -1), row["similarity"])
                scores[rid]+=weight/(60+rank); reasons[rid].add(channel)
                if "span_start" in row: spans.setdefault(rid,(row["span_start"],row["span_end"]))
            counts[channel]=counts.get(channel,0)+len(seen)

        semantic = self._ensure_semantic()
        futures=[]
        for variant in variants:
            lexical = self.store.search(variant,limit=pool_size,source=source,after=after,before=before,
                                        allowed_ids=allowed,include_history=include_history)
            add("keyword",lexical["episodes"])
            add("claim_keyword",[{"id":c["record_id"]} for c in lexical["claims"]])
            claim_rows.update({c["id"]:c for c in lexical["claims"]})
            if semantic:
                futures.append(("semantic",self.pool.submit(semantic.candidates,variant,pool_size,allowed)))
            if self.hindsight:
                futures.append(("hindsight",self.pool.submit(self.hindsight.candidates,variant,depth,source)))
        for channel,future in futures:
            began = time.monotonic()
            try:
                add(channel,future.result(timeout=max(0.01,25-(time.monotonic()-started))))
                if debug_enabled(): engines[channel]=round((time.monotonic()-began)*1000)
            except Exception as error:
                future.cancel()
                failures[channel]=type(error).__name__ + ": retrieval incomplete"
                if debug_enabled(): engines[channel]="fail"

        # Graph activation exposes source-backed neighbours as leads; it never asserts they
        # are the answer or the same person. Two bounded sources feed one fusion channel:
        # (1) confirmed identity bridges (a person's own time-valid history) and (2) multi-hop
        # personalised PageRank diffusion across shared entities.
        neighbours=[]
        if expand_entities and depth!="fast":
            seed_ids=sorted(scores,key=lambda rid:(-scores[rid],rid))[:self.graph_seed_top]
            seeds={rid:scores[rid] for rid in seed_ids}
            with self.store.connect() as db:
                for rid in seed_ids:
                    for edge in db.execute("SELECT DISTINCT entity_id FROM entity_links WHERE record_id=? LIMIT ?",(rid,self.graph_record_degree)):
                        owners=db.execute("""SELECT i.person_id FROM identity_edges i JOIN records evidence ON evidence.id=i.record_id
                          JOIN records seed ON seed.id=? WHERE i.account_id=? AND i.status='confirmed' AND evidence.deleted=0
                          AND (i.valid_from IS NULL OR seed.occurred_at>=i.valid_from)
                          AND (i.valid_to IS NULL OR seed.occurred_at<i.valid_to) LIMIT 2""",(rid,edge["entity_id"]))
                        for owner in owners:
                            related=self.store.related_ids(owner[0])
                            if allowed is not None:related &= allowed
                            # Deterministic bounded leads; explicit person search/browse
                            # can investigate the complete time-valid contact history.
                            for linked in sorted(related)[:16]:
                                if linked!=rid:
                                    neighbours.append({"id":linked,"via_entity":owner[0],"from_record":rid})
            seen_neighbours={item["id"] for item in neighbours}
            for item in self._propagate_graph(seeds, allowed):
                if item["id"] not in seen_neighbours:
                    neighbours.append({"id":item["id"],"via_entity":item["via_entity"],"from_record":item["from_record"]})
            add("graph_propagation",neighbours,weight=self.graph_weight)

        ordered=sorted(scores,key=lambda rid:(-scores[rid],rid))
        if entity_id:
            still_related=self.store.related_ids(entity_id)
            ordered=[rid for rid in ordered if rid in still_related]
        ordered,scores=self._apply_rerank(query,ordered,scores)
        ordered=self._apply_recency(ordered,scores)
        episodes=[]
        # Rehydrate after external/network work: new tombstones cannot be returned
        # just because an older index or external engine still remembers the ID.
        with self.store.connect() as db:
            for rid in ordered:
                if not include_history and db.execute('SELECT 1 FROM record_visibility WHERE record_id=? AND hidden=1',(rid,)).fetchone():continue
                row=db.execute("SELECT id,source,source_id,NULLIF(occurred_at,'') AS occurred_at,kind,text FROM records WHERE id=? AND deleted=0",(rid,)).fetchone()
                if not row: continue
                item=dict(row); original=item["text"]
                assessment=self.relevance.assess(variants,original,similarities.get(rid))
                if not assessment["accepted"]:
                    rejected += 1
                    continue
                item["relevance"]=assessment
                start,end=spans.get(rid,(0,min(2400,len(original))))
                item.update(text=original[start:min(end,start+2400)],truncated=(start>0 or end<len(original) or end-start>2400),
                            span_start=start,span_end=min(end,start+2400),retrieved_by=sorted(reasons[rid]),rrf_score=round(scores[rid],6))
                episodes.append(item)
                if len(episodes)>=limit: break
            selected={r["id"] for r in episodes}
            # Pull relevant claims even when only their supporting source matched
            # semantically. Status and validity remain explicit in every response.
            for rid in selected:
                status="c.status!='retracted'" if include_history else "c.status='active'"
                for row in db.execute("SELECT c.*,r.source FROM claims c JOIN records r ON r.id=c.record_id WHERE r.deleted=0 AND c.record_id=? AND "+status+" ORDER BY c.created_at DESC LIMIT 30",(rid,)):
                    claim_rows[row["id"]]=dict(row)
            claims=[]
            for claim in claim_rows.values():
                if claim["record_id"] not in selected: continue
                current=db.execute("SELECT c.* FROM claims c JOIN records r ON r.id=c.record_id WHERE c.id=? AND r.deleted=0 AND c.status!='retracted'",(claim["id"],)).fetchone()
                if current and (include_history or current["status"]=="active"):
                    claims.append(dict(current))
        state=self.status()
        if debug_enabled():
            LOG.debug("search depth=%s variants=%s engines_ms=%s candidates=%s rejected=%s failures=%s ms=%s",
                      depth, len(variants), engines or "-", counts or "-", rejected, failures or "-",
                      round((time.monotonic()-started)*1000))
        return {"episodes":episodes,"claims":claims[:limit],"retrieval":"hybrid_rrf","coverage":state["sources"],
                "retrieval_status":"candidates_found" if episodes else ("retrieval_incomplete" if failures or (self.semantic and not state["semantic"].get("ready",False)) else "no_relevant_evidence"),
                "evidence_sufficiency":"not_established",
                "generation":self.store.generation()["generation"],"diagnostics":{"depth":depth,"query_count":len(variants),"candidates":counts,"failures":failures,"relevance_rejected":rejected,
                               "elapsed_ms":round((time.monotonic()-started)*1000),"semantic":state["semantic"],"hindsight":state["hindsight"]},
                "connections":[x for x in neighbours if x["id"] in selected],
                "next_steps":["Read evidence for selected IDs.","Use deep with query variants for indirect questions.","Use browse cursors for exhaustive stored-history inspection."],
                "warning":"Rank is relevance, not confidence or proof. Entity neighbours are leads. Inspect claim dates/status. Incomplete indexing, source coverage or search failure means unknown, not absent."}

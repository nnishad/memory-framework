"""Progressive hybrid retrieval, RRF fusion, and canonical evidence hydration."""
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from .common import required_text, timestamp
from .relevance import RelevanceGate


class Hybrid:
    def __init__(self, store, config=None, semantic=None, hindsight=None, start=True):
        self.store, self.config = store, config or {}
        self.relevance = RelevanceGate(self.config.get("relevance"))
        self.semantic, self.hindsight = semantic, hindsight
        self.errors, self.threads = {}, []
        self.stop = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="memory-search")
        for name, cls in (("semantic", "SemanticIndex"), ("hindsight", "Hindsight")):
            cfg = self.config.get(name, {})
            if cfg.get("enabled") and getattr(self, name) is None:
                try:
                    if name == "semantic":
                        from .semantic import SemanticIndex
                        self.semantic = SemanticIndex(store, cfg)
                    else:
                        from .hindsight import Hindsight
                        self.hindsight = Hindsight(store, cfg)
                except Exception as error:
                    self.errors[name] = type(error).__name__ + ": initialization failed; check model/dependencies/configuration"
        if start:
            for component in (self.semantic,self.hindsight):
                if component:
                    def work(engine=component):
                        delay = 0
                        while not self.stop.wait(delay):
                            try:
                                delay = 0.05 if engine.sync(batch=1) else 2
                            except Exception as error:
                                engine.last_error = type(error).__name__ + ": indexing failed; retry pending"
                                delay = 10
                    thread = threading.Thread(target=work, daemon=True, name="memory-index")
                    thread.start(); self.threads.append(thread)

    def close(self):
        self.stop.set()
        self.pool.shutdown(wait=True, cancel_futures=True)
        deadline=time.monotonic()+125
        for thread in self.threads:
            thread.join(timeout=max(0,deadline-time.monotonic()))

    def status(self):
        result = self.store.status()
        result.update(backend="hybrid_rrf", semantic_embeddings=self.semantic is not None,
                      semantic=self.semantic.status() if self.semantic else {"enabled":False},
                      hindsight=self.hindsight.status() if self.hindsight else {"enabled":False},
                      initialization_errors=self.errors,
                      capabilities=["keyword","temporal_filters","account_identity","evidence","pagination","query_variants"])
        if self.semantic:
            result["capabilities"].append("semantic")
        if self.hindsight:
            result["capabilities"].append("hindsight_multistrategy")
        return result

    def search(self, query, limit=8, entity_id=None, source=None, after=None, before=None,
               include_history=False, depth="balanced", queries=None, expand_entities=True):
        required_text(query,"query",4000)
        if type(limit) is not int or not 1 <= limit <= 30:
            raise ValueError("limit must be 1..30; use browse cursors for complete stored evidence")
        if depth not in {"fast","balanced","deep"}:
            raise ValueError("depth must be fast, balanced or deep")
        if queries is not None and (not isinstance(queries,list) or len(queries)>4):
            raise ValueError("queries must contain at most four alternative or subquestion queries")
        variants = list(dict.fromkeys([query]+[required_text(q,"query variant",4000) for q in (queries or [])]))
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
        scores, reasons, spans, claim_rows = defaultdict(float), defaultdict(set), {}, {}
        failures, counts = dict(self.errors), {}
        similarities = {}; rejected = 0

        def add(channel, rows, weight=1):
            seen=set()
            for rank,row in enumerate(rows,1):
                rid=row["id"]
                if (allowed is not None and rid not in allowed) or rid in seen: continue
                seen.add(rid)
                if channel == "semantic" and "similarity" in row:
                    similarities[rid] = max(similarities.get(rid, -1), row["similarity"])
                scores[rid]+=weight/(60+rank); reasons[rid].add(channel)
                if "span_start" in row: spans.setdefault(rid,(row["span_start"],row["span_end"]))
            counts[channel]=counts.get(channel,0)+len(seen)

        futures=[]
        for variant in variants:
            lexical = self.store.search(variant,limit=pool_size,source=source,after=after,before=before,
                                        allowed_ids=allowed,include_history=include_history)
            add("keyword",lexical["episodes"])
            add("claim_keyword",[{"id":c["record_id"]} for c in lexical["claims"]])
            claim_rows.update({c["id"]:c for c in lexical["claims"]})
            if self.semantic:
                futures.append(("semantic",self.pool.submit(self.semantic.candidates,variant,pool_size,allowed)))
            if self.hindsight:
                futures.append(("hindsight",self.pool.submit(self.hindsight.candidates,variant,depth,source)))
        for channel,future in futures:
            try:
                add(channel,future.result(timeout=max(0.01,25-(time.monotonic()-started))))
            except Exception as error:
                future.cancel()
                failures[channel]=type(error).__name__ + ": retrieval incomplete"

        # Entity expansion exposes source-backed neighbours, never asserts they are
        # answers or the same person. Bounded one hop prevents group fan-out floods.
        neighbours=[]
        if expand_entities and depth!="fast":
            seeds=sorted(scores,key=scores.get,reverse=True)[:3]
            with self.store.connect() as db:
                for rid in seeds:
                    for edge in db.execute("SELECT entity_id,relation FROM entity_links WHERE record_id=? LIMIT 4", (rid,)):
                        rows=db.execute("""SELECT r.id FROM entity_links l JOIN records r ON r.id=l.record_id
                            WHERE l.entity_id=? AND r.deleted=0 AND r.id!=? ORDER BY r.occurred_at DESC LIMIT 8""", (edge["entity_id"],rid))
                        for row in rows:
                            if allowed is None or row[0] in allowed:
                                neighbours.append({"id":row[0],"via_entity":edge["entity_id"],"from_record":rid})
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
            add("entity_neighbour",neighbours,weight=0.25)

        ordered=sorted(scores,key=lambda rid:(-scores[rid],rid))
        if entity_id:
            still_related=self.store.related_ids(entity_id)
            ordered=[rid for rid in ordered if rid in still_related]
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
        return {"episodes":episodes,"claims":claims[:limit],"retrieval":"hybrid_rrf","coverage":state["sources"],
                "retrieval_status":"candidates_found" if episodes else ("retrieval_incomplete" if failures or (self.semantic and not state["semantic"].get("ready",False)) else "no_relevant_evidence"),
                "evidence_sufficiency":"not_established",
                "generation":self.store.generation()["generation"],"diagnostics":{"depth":depth,"query_count":len(variants),"candidates":counts,"failures":failures,"relevance_rejected":rejected,
                               "elapsed_ms":round((time.monotonic()-started)*1000),"semantic":state["semantic"],"hindsight":state["hindsight"]},
                "connections":[x for x in neighbours if x["id"] in selected],
                "next_steps":["Read evidence for selected IDs.","Use deep with query variants for indirect questions.","Use browse cursors for exhaustive stored-history inspection."],
                "warning":"Rank is relevance, not confidence or proof. Entity neighbours are leads. Inspect claim dates/status. Incomplete indexing, source coverage or search failure means unknown, not absent."}

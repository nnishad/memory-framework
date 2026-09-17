"""Unified lifecycle retirement shared by canonical and derived memory.

One transaction-aware operation hides retired evidence and invalidates every
dependent artifact (awareness results, learning objects and curated entries) so
that no path can retire evidence while silently leaving stale conclusions
current. A single "live and visible" predicate governs the creation of new
derived memory, and an idempotent repair pass cleans up artifacts that were made
stale before retirement was unified.

Every function here operates inside the caller's SQLite transaction; none of
them open a connection of their own except the top-level repair pass.
"""


def _descendants(db, root):
    return {r[0] for r in db.execute(
        "WITH RECURSIVE d(id) AS (SELECT ? UNION SELECT child_id FROM record_dependencies"
        " JOIN d ON parent_id=d.id) SELECT id FROM d", (root,))}


def live_and_visible(db, record_id):
    """Evidence is usable for current memory only when it is not deleted or hidden."""
    return bool(db.execute(
        "SELECT 1 FROM records WHERE id=? AND deleted=0"
        " AND NOT EXISTS(SELECT 1 FROM record_visibility WHERE record_id=? AND hidden=1)",
        (record_id, record_id)).fetchone())


def require_live_evidence(db, record_ids, *, message="Evidence must be live and visible"):
    for rid in record_ids:
        if not live_and_visible(db, rid):
            raise ValueError(message)


def retire(db, store, record_id, *, replacement=None, exclude_replacement=False):
    """Retire ``record_id`` and its derived descendants inside the caller's transaction.

    Hides retired evidence and invalidates dependent awareness results, learning
    artifacts and curated entries. When ``exclude_replacement`` is set, descendants
    that are also reachable from ``replacement`` stay visible so a source
    replacement never hides its own new head. Returns the hidden record ids.
    """
    from . import awareness
    from .learning import invalidate as invalidate_learning
    from .curated import invalidate as invalidate_curated
    hidden = _descendants(db, record_id)
    if exclude_replacement and replacement:
        hidden -= _descendants(db, replacement)
    for rid in hidden:
        awareness.invalidate_record(db, rid)
        db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,?)", (rid, replacement))
        invalidate_learning(db, record_id=rid)
        invalidate_curated(db, store, rid)
    return hidden


def repair_retired_dependencies(store):
    """Idempotent pass retiring active artifacts that already cite retired evidence.

    Scans learning objects and curated entries whose evidence is deleted or hidden
    and invalidates them. Restoring source visibility later never reactivates a
    conclusion, so a subsequent pass finds nothing to do. Returns counts by kind.
    """
    from .learning import invalidate as invalidate_learning
    from .curated import invalidate as invalidate_curated
    report = {"learning": 0, "curated": 0}
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        retired = {r[0] for r in db.execute(
            "SELECT id FROM records WHERE deleted=1"
            " UNION SELECT record_id FROM record_visibility WHERE hidden=1")}
        if retired:
            learning_seeds = set()
            for rid in retired:
                for row in db.execute(
                        "SELECT o.id FROM learning_objects o JOIN learning_evidence e ON e.object_id=o.id"
                        " WHERE e.record_id=? AND o.state IN ('active','recorded','candidate')", (rid,)):
                    learning_seeds.add(row[0])
            if learning_seeds:
                invalidate_learning(db, objects=sorted(learning_seeds))
                report["learning"] = len(learning_seeds)
            offending = set()
            for rid in retired:
                if db.execute(
                        "SELECT 1 FROM curated_entries WHERE retired_version IS NULL"
                        " AND EXISTS(SELECT 1 FROM json_each(curated_entries.evidence_ids) WHERE value=?)",
                        (rid,)).fetchone():
                    offending.add(rid)
            for rid in sorted(offending):
                invalidate_curated(db, store, rid)
                report["curated"] += 1
            if report["learning"] or report["curated"]:
                store.audit(db, "retirement_repair", "lifecycle", dict(report))
    return report

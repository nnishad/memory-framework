# Review of the R1–R7 fixes

Reviewed revision: `274496b`, compared with `e620fff`.

The implementations address the original empty-index, per-model backlog, publication bookkeeping, prefetch-response and incomplete-page cases. Three further boundary gaps were reproduced. No runtime changes were made during this review.

## 1. P1 — Failed construction loses the handle needed for cleanup

Locations: `personal_memory/asgi.py:98`; `personal_memory/service.py:54`.

`Application` assigns `self.service` only after `MemoryService(...)` returns. When construction fails after creating resources, `MemoryService.__init__` calls cleanup, logs a cleanup failure, and rethrows the original exception. The application never receives the partially constructed service. Its teardown sees `service=None`, so it closes the managed runtime and releases the service lease even though the failed object retains unfinished resources.

**Reproduction:** inject a late `_construct` failure after registering a closable whose `close()` raises. The application reports startup failure; `app.service` is `None`; the partially constructed object still has one unclosed resource; a second `ProcessLease(service.lock)` succeeds.

This bypasses the new dependency-aware teardown rather than exercising it. Existing tests start with an already-assigned service, so they do not cover this ownership transfer.

**Core correction:** establish an externally retained owner before starting resources, for example an assigned service with an explicit start phase or a construction resource owner shared with the application. An exception must not erase the only cleanup handle. Apply the same rule to managed-runtime construction/start. Preserve the original diagnostic while retaining dependencies and ownership until cleanup succeeds.

**Regression:** fail after a real or gated worker starts, fail its first close, verify both cleanup handle and process lease remain held, then release the gate and retry cleanup successfully.

## 2. P2 — Shutdown does not drain requests still awaiting their bodies

Locations: `personal_memory/asgi.py:131`, `:185`, `:188`.

The request checks `accepting`, then awaits its HTTP body before submitting to the executor. Shutdown drains only work already submitted to that executor. An authenticated request waiting on `receive()` is not counted. Shutdown closes the service and reports completion; when the request resumes it attempts `self.service.dispatch` against `None` and returns HTTP 500.

**Reproduction:** start an HTTP request and gate body delivery; run shutdown; release the body. Observed `lifespan.shutdown.complete` followed by HTTP 500 for the accepted request.

The application needs to distinguish accepted asynchronous handlers from executor tasks. Waiting synchronously on the event-loop thread would not solve this: those handlers need the loop to finish receiving their bodies.

**Core correction:** track admitted HTTP handlers across body read, execution and response using an asynchronous lifecycle barrier. Stop admission first; await admitted handlers within a defined budget; then drain the executor and close service resources. If the policy cancels a handler, return a deliberate stopping response and guarantee it cannot dispatch later. Keep each handler bound to its admitted lifecycle so an old request cannot dispatch into a restarted service.

**Regression:** gate body receipt and response sending independently; test disconnect, timeout, executor saturation and shutdown/restart. Assert that no accepted handler executes after its lifecycle has closed and that shutdown does not block the event loop needed to drain it.

## 3. P2 — Accelerator recovery can disappear when the triggering record is forgotten

Locations: `personal_memory/semantic.py:515`; `personal_memory/service.py:402`.

An ambiguous insertion marks `_index_unhealthy=True`, but rebuild happens only inside `_publish` for a subsequent indexing obligation. Forgetting the failed record removes its model work and durable chunks. When that was the last obligation, subsequent idle `sync()` calls never rebuild or clear the suspect accelerator.

The new health state is also not integrated into the service readiness check: `MemoryService.ready()` checks errors and queue counts, but ignores `accelerator_healthy=False` and the semantic engine's `ready=False`.

**Reproduction:** inject an insertion that mutates then raises; forget the affected record; call sync repeatedly. Observed zero failed records, zero pending records, zero pending retirements, `accelerator_healthy=False`, and semantic `ready=False`. Calling the service readiness method over that backend returns `ready=True`.

This leaves recovery dependent on unrelated future ingestion or process restart and gives the operator conflicting health signals.

**Core correction:** schedule accelerator recovery independently of record work, with bounded retry/backoff. A rebuild of an empty live archive must replace/clear the suspect index and its maps, not leave the old object installed. Integrate accelerator health and incomplete registration into service readiness. Avoid rebuilding on every search request.

**Regression:** fail publication, forget the only failed record, stop ingesting, and prove background sync restores a healthy empty index. Repeat while valid records remain, and assert their candidates survive recovery. Verify `/v1/ready` remains false until recovery succeeds, including empty rebuild and rebuild failure.

## Additional repair-path observation

`semantic._ensure_seq_singleton` does not include `semantic_model_work.seq` when computing the highest durable revision. A missing singleton with only model work at revision 100 is repaired to zero. This is a narrower repair-path gap than the normal-operation findings above, but the new table should participate in the clock invariant and its migration tests.

## Validation

The reproductions used temporary databases, fault-injected resources and the repository's fake accelerator. They did not access Gmail, private mail, live models or deployed services. Full-suite results are appended after completion. Existing untracked review artifacts and `stub_hermes_contract.py` were preserved; the stub was not used as the Hermes contract.

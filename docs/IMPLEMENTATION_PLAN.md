# Capability coverage — 0.7.0rc1

The former implementation backlog now has concrete framework modules and extension points.
The table distinguishes implemented behavior from deployment qualification and optional product scope.

| Workstream | Implemented | Boundary |
| --- | --- | --- |
| Outcome learning | Evidence-backed outcomes, immutable proposals, evaluation/promotion and retraction | Observable outcome reports need truthful source adapters; no arbitrary task-success oracle |
| Evaluation | Immutable suites, active-baseline replay, subprocess runner, strict JSON comparison, scoped automatic promotion and crash reconciliation | Real-model/held-out task quality must be measured; fixture performance is not model quality |
| Retrieval | Progressive rounds, optional model planner/reranker, duplicate context suppression, explicit filters and budgets | No universal calibrated abstention or guarantee of complete inference |
| Consolidation | Durable source cursor, full-source span partitioning, exact snapshots, model/extractive adapters, schema validation, review and dependency invalidation | Extractive default does not invent facts; model proposals remain unverified |
| Beliefs | Structured values, dates, scoped preferences, conflict sets, explicit correction and domain coverage gates | No automated theorem proving or silent selection of truth from conflicting sources |
| Procedures | Evaluated lesson -> administrator-bound capability -> authorized execution job -> output validation | No compilation/execution of arbitrary model-generated code; external effect idempotency belongs to capability adapter |
| Tasks | Versioned lifecycle, dependencies, deadlines, leased reminder outbox, retry/quarantine and durable terminal recovery | Actual authorized channel delivery uses an installed host adapter |
| Relationships | Dated typed graph links and bounded traversal, plus existing temporal identity ownership | Associations are not identity/cause proof; speculative merges are not automatic |
| Personalization | Explicit/inferred origin, contextual matching, validity and preference priority | Preference extraction quality and unusual exceptions need archive-specific evaluation |
| Quality management | Coverage, worker/job status, feedback, duplicate suppression, invalidation and quarantine | No automatic destructive aging/deletion or unvalidated popularity-based truth ranking |
| Structured domains | Typed metric registry, unit validation/conversion, numerical aggregate API | New domains require a versioned definition and source mapping; OCR/audio and continuous provider sync remain connector extensions |
| Proactive use | Due-event bridge, configured recipient/capability authority, source coverage gates and task recovery | No unsolicited destination inference or new permissions from remembered text |

See FRAMEWORK.md for actual configuration and API contracts. This is a release candidate,
not a claim of AGI, production certification, omniscience, universal inference accuracy or
that every Hermes action mechanically consults memory. The intended deployment must qualify
selected models, representative personal data, full-volume performance, real delivery/capability
adapters and host-loss recovery. Those are environment-specific acceptance gates, not fictitious
successful tests in this package.

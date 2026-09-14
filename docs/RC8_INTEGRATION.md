# RC8 default Hindsight integration

Target: Hermes Agent v2026.9.11 and Hindsight 0.9.2.

RC8 turns the previous Hindsight HTTP adapter into a zero-configuration service feature.
Hindsight's 0.9.2 slim API, embedded-db and local-ONNX packages are required dependencies.
This avoids installing unused Torch, MLX and local-LLM stacks. Normal setup writes a managed,
profile-specific configuration covering all canonical sources, and loading an older profile
forces Hindsight on even when it contains the former `enabled:false` opt-out.

The ASGI lifespan starts Hindsight before constructing the hybrid retrieval engine and stops
the owned daemon after the indexing worker closes. The reference transport uses the same managed
lifecycle. A stable profile-derived backend ID keeps the durable retain/deletion journal valid
when the embedded daemon chooses a different loopback port after restart.

Provider selection is automatic. Existing `HINDSIGHT_API_LLM_*` settings are inherited. An
authenticated Hermes profile uses Hindsight's native Nous Portal provider; otherwise the runtime
selects `none`, which provides local chunk retention and hybrid recall without an LLM or API key.
The canonical SQLite store remains authoritative. Hindsight candidates are rehydrated locally,
relevance-gated and checked against current visibility and deletion state before Hermes sees them.

Readiness reports retain and deletion backlog or worker errors, and doctor now verifies the
Hindsight dependency and active engine. An external deployment remains supported as an operational
override (`managed:false` plus `url`), but Hindsight itself cannot be disabled.

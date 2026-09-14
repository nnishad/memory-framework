# Current release: 0.7.0rc1

Core framework modules are implemented and connected: ingestion, recall, structured knowledge,
consolidation, evaluation, governed promotion, capabilities, tasks and recovery. See
IMPLEMENTATION_PLAN.md for the capability-by-capability boundary and FRAMEWORK.md for configuration.

Deployment qualification remains: actual models and source mappings, archive recall/conflicts/
identity evaluation, full-volume indexing and latency, real delivery/capability adapters,
credential/key custody, host-loss recovery and retained deletion/retraction history.

Additional product extensions include continuous source synchronizers, attachment/audio/OCR
adapters and specialized domain analyzers. The strict ingestion contract accepts new sources
without requiring those extensions to be built into the memory core.

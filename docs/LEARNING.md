# Governed learning — current release 0.8.0rc8

The complete current specification is [FRAMEWORK.md](FRAMEWORK.md). The earlier 0.6.0a1
learning substrate is now connected to immutable evaluation suites, active-baseline replay,
automatic scope policies, consolidation jobs, typed knowledge, tasks and installed capabilities.

The loop is outcome -> candidate -> independent evaluation -> promotion -> applicability-aware
use -> feedback/retraction. A candidate cannot approve itself. A passing evaluation binds exact
candidate/suite/adapter/baseline versions. Executed suites compare output to independently stored
expected JSON; they do not accept a model's own success declaration. Automatic promotion exists
only for explicitly configured scope/suite policies. Procedure execution still requires a
separate capability grant and administrator-bound arguments.

Source deletion invalidates downstream artifacts. The current independent intent ledger also
replays lesson retractions and terminal task state when restoring an older snapshot. It must be
available and supplied to isolated restore; an old backup alone cannot reconstruct later events.
No process can retract already delivered external side effects or securely erase all old backups.

The implementation is tested with synthetic tasks and protocol fixtures. It changes memory and
retrieval-guided behavior, not model weights. Real-model learning effectiveness remains a measured
deployment property.

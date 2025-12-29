# Core Frozen

As of this commit:

- SQL semantics are frozen
- Logical IR is frozen
- Optimizer rules are frozen
- Execution operator contracts are frozen

All future work must preserve:

- Existing tests
- Existing invariants
- Existing public APIs

Storage is allowed to evolve freely under ScanExec

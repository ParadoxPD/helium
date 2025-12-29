# Helium Architecture

Helium is a lightweight, embedded analytical database engine designed with a
clean separation between query semantics, execution, and storage.

The system is deliberately layered so that storage can evolve independently
from the query engine.

---

## High-Level Layers

```
SQL / NQL
    ↓
Parser
    ↓
Binder
    ↓
Logical Plan
    ↓
Optimizer
    ↓
Physical Execution
    ↓
Storage Interface
```

---

## Components

### 1. Frontend (SQL / NQL)

- Parses SQL into an AST
- Supports SELECT, WHERE, JOIN, ORDER BY, LIMIT
- EXPLAIN and EXPLAIN ANALYZE are handled as first-class statements

### 2. Binder

- Resolves table and column names
- Converts unbound column references into fully qualified references
- Detects ambiguous and unknown columns early
- Produces a **bound representation** that is safe for optimization

### 3. Logical Plan

- Declarative description of the query
- Contains operators like:
  - Scan
  - Filter
  - Project
  - Join
  - Sort
  - Limit
- Independent of execution strategy and storage layout

### 4. Optimizer

- Rule-based optimizer
- Applies transformations such as:
  - Predicate pushdown
  - Projection pruning
  - Redundant projection elimination
- Preserves semantics while improving performance

### 5. Physical Execution

- Pull-based iterator model
- Operators implement:
  - open()
  - next() ⇾ `Option<Row>`
  - close()
- Execution is streaming and composable
- LIMIT short-circuits upstream operators

### 6. Storage Interface

- Execution layer interacts with storage only through Scan
- Storage provides row streams
- No execution logic depends on physical storage layout

---

## Design Principles

- Separation of concerns
- Explicit invariants
- Minimal magic
- Test-driven integration
- Storage-agnostic execution

Helium intentionally avoids premature optimization and focuses on correctness
and clarity before introducing persistence.

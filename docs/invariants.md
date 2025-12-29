# Engine Invariants

Helium enforces strict invariants between layers to prevent subtle bugs.

Violating these invariants is considered a bug.

---

## Parsing & Binding

- All column references must be bound before optimization
- Ambiguous column references are rejected
- Unknown tables or columns are rejected

---

## Logical Plan

- Logical plans must not contain unbound expressions
- Expressions must be pure and deterministic
- Logical plans are immutable after construction

---

## Optimization

- Optimizations must preserve semantics
- Projection pruning must not remove columns required by predicates
- Predicate pushdown must not cross semantic boundaries (e.g., LIMIT)

---

## Execution

- Execution must never see `Expr::Column`
- Only `Expr::BoundColumn` is allowed at runtime
- Execution operators must not mutate input rows
- LIMIT must short-circuit upstream operators

---

## Row Shape

- Rows entering execution are fully qualified (table.column)
- Rows leaving Project are unqualified (column)
- Output schema is determined solely by Project

---

## EXPLAIN

- EXPLAIN must never execute a query
- EXPLAIN ANALYZE executes but does not return rows
- Execution must never see an Explain node

---

## Execution Invariants

- ScanExec outputs base-qualified columns
- AliasExec outputs alias-qualified columns
- ProjectExec outputs unqualified columns

---

## Storage Invariants

- Storage is alias-agnostic
- Storage never sees SQL-level aliases

---

These invariants are enforced through:

- Assertions
- Panic-on-violation
- Integration tests

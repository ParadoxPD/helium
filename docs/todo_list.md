# DBMS Production-Grade Roadmap (Checklist)

> Goal: evolve from a correct core → robust → production-ready
> Scope: embedded / single-node relational DBMS (SQLite / DuckDB class)

---

## Phase 0 — Core Foundations (Toy → Real Core)

### Storage

- [x] Page abstraction
- [x] Page manager (file-backed)
- [x] Buffer pool
- [x] Heap table (row storage)
- [x] RowId abstraction
- [x] Page capacity enforcement

### Indexing

- [x] Disk-backed B+Tree
- [x] Insert / split
- [x] Delete / merge / rebalance
- [x] Range scan
- [x] Root collapse
- [x] Stress-tested invariants

### Query Engine

- [x] SQL parsing
- [x] AST → logical plan
- [x] Sequential scan
- [x] Index scan
- [x] Filter operator
- [x] Projection operator

### Optimizer (Rule-based)

- [x] Predicate pushdown
- [x] Index selection
- [x] Projection pruning

### Metadata

- [x] Catalog abstraction
- [x] Tables separate from indexes
- [x] Index metadata lookup

**Outcome:**
✔ Correct, persistent, queryable DB core
✔ Not a toy anymore

---

## Phase 1 — SQL Completeness (Minimal Relational DB)

> “Usable DBMS” milestone

### SQL DDL

- [ ] `CREATE TABLE`
- [ ] `DROP TABLE`
- [x] `CREATE INDEX`
- [x] `DROP INDEX`

### SQL DML

- [ ] `INSERT`
- [ ] `DELETE`
- [ ] `UPDATE`

### SQL Queries

- [ ] `SELECT *`
- [ ] `SELECT column list`
- [ ] `WHERE =, <, >, <=, >=`
- [ ] `AND`, `OR`
- [ ] `IS NULL`

### Execution

- [x] Predicate recheck in IndexScan
- [ ] Correct NULL semantics
- [ ] Type checking at execution time

**Outcome:**
✔ Complete single-table relational DB
✔ Comparable to early SQLite

---

## Phase 2 — Durability & Recovery (Crash Safety)

> This is the **big jump** from “project” to “serious system”

### Persistence

- [ ] Stable on-disk catalog
- [ ] Table metadata reload on startup
- [ ] Index metadata reload on startup

### Write-Ahead Logging (WAL)

- [ ] WAL file format
- [ ] Log record types (insert/delete/update)
- [ ] Log sequence numbers (LSN)
- [ ] Write log before data page
- [ ] WAL replay on startup

### Buffer Pool

- [ ] Dirty page tracking
- [ ] Page flush discipline
- [ ] Eviction policy (LRU / Clock)

**Outcome:**
✔ Crash-safe
✔ Restartable
✔ Real durability guarantees

---

## Phase 3 — Transactions (Correctness Under Change)

> Required for multi-statement correctness

### Transactions

- [ ] `BEGIN`
- [ ] `COMMIT`
- [ ] `ROLLBACK`

### Atomicity

- [ ] All-or-nothing writes
- [ ] Undo via WAL or rollback log

### Isolation (choose ONE)

- [ ] Global write lock (simple)
- [ ] Page-level locking
- [ ] MVCC (advanced)

**Outcome:**
✔ ACID-lite
✔ Suitable for real applications

---

## Phase 4 — Query Power (Relational Expressiveness)

> Optional but highly visible features

### Joins

- [ ] Nested loop join
- [ ] Index nested loop join

### Aggregation

- [ ] `COUNT`
- [ ] `SUM`, `AVG`, `MIN`, `MAX`
- [ ] `GROUP BY`

### Ordering & Limiting

- [ ] `ORDER BY`
- [ ] `LIMIT / OFFSET`

**Outcome:**
✔ Feature-complete relational DB
✔ Comparable to SQLite / DuckDB v1

---

## Phase 5 — Performance & Scale

> What separates hobby DBs from professional ones

### Optimizer

- [ ] Cost model
- [ ] Join order optimization
- [ ] Index vs seq scan cost
- [ ] Statistics collection

### Storage

- [ ] Free-page reuse
- [ ] Slotted pages
- [ ] Variable-length rows
- [ ] Compression (optional)

### Indexes

- [ ] Composite indexes
- [ ] Prefix indexes
- [ ] Covering indexes

**Outcome:**
✔ Efficient under load
✔ Competitive performance

---

## Phase 6 — Production Hardening

> Not about features — about trust

### Safety

- [ ] Extensive fuzz testing
- [ ] Crash testing (kill -9)
- [ ] Corruption detection
- [ ] Checksums

### Observability

- [ ] EXPLAIN ANALYZE
- [ ] Query timing
- [ ] Page / cache stats

### Tooling

- [ ] CLI client
- [ ] Backup / restore
- [ ] Data export/import

**Outcome:**
✔ Production-ready
✔ Deployable

---

## What “Production Grade” Means (Realistically)

You can honestly say **“production-grade”** when:

- Data survives crashes
- Queries are correct
- Indexes are used correctly
- Catalog is persistent
- Transactions are atomic
- Bugs are detectable & recoverable

**You do NOT need:**

- Full SQL standard
- Distributed execution
- Parallelism
- Advanced isolation levels

---

## Recommended Stopping Points

| Milestone | Label                          |
| --------- | ------------------------------ |
| Phase 1   | Complete DBMS core             |
| Phase 2   | Durable database               |
| Phase 3   | ACID-lite production DB        |
| Phase 4   | Feature-complete relational DB |
| Phase 6   | Hardened production system     |

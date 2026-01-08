# Helium Development Roadmap

> Goal: Build a production-grade, embeddable DBMS with ORM-free querying
> Target: SQLite-class reliability + DuckDB-class analytics + Native Query Language

---

## Phase 0 — Core Foundations (COMPLETED)

### Storage ✓

- [x] Page abstraction
- [x] Page manager (file-backed)
- [x] Buffer pool
- [x] Heap table (row storage)
- [x] RowId abstraction
- [x] Page capacity enforcement

### Indexing ✓

- [x] Disk-backed B+Tree
- [x] Insert / split
- [x] Delete / merge / rebalance
- [x] Range scan
- [x] Root collapse
- [x] Stress-tested invariants

### Query Engine ✓

- [x] SQL parsing (with tokenizer)
- [x] AST → logical plan
- [x] Sequential scan
- [x] Index scan
- [x] Filter operator
- [x] Projection operator
- [x] Join operator (nested loop)
- [x] Sort operator
- [x] Limit operator

### Optimizer ✓

- [x] Predicate pushdown
- [x] Index selection
- [x] Projection pruning
- [x] Constant folding

### Metadata ✓

- [x] Catalog abstraction
- [x] Tables separate from indexes
- [x] Index metadata lookup

---

## Phase 1 — SQL Completeness (IN PROGRESS - 95% Complete)

### SQL DDL ✓

- [x] `CREATE TABLE`
- [x] `DROP TABLE`
- [x] `CREATE INDEX`
- [x] `DROP INDEX`
- [ ] `ALTER TABLE` (add/drop columns)
- [ ] Column constraints (PRIMARY KEY, UNIQUE, CHECK)
- [ ] Default values for columns

### SQL DML ✓

- [x] `INSERT` (single row)
- [x] `INSERT` (multiple rows)
- [x] `DELETE`
- [x] `UPDATE`
- [ ] `INSERT ... SELECT`
- [ ] `UPDATE ... FROM` (join updates)

### SQL Queries

- [x] `SELECT *`
- [x] `SELECT column list`
- [x] `WHERE =, <, >, <=, >=, !=`
- [x] `AND`, `OR`, `NOT`
- [x] `IS NULL`, `IS NOT NULL`
- [x] `ORDER BY` (ASC/DESC)
- [x] `LIMIT`
- [ ] `OFFSET`
- [ ] `DISTINCT`
- [ ] `IN` operator
- [ ] `BETWEEN` operator
- [ ] `LIKE` / pattern matching
- [ ] String functions (UPPER, LOWER, SUBSTR, etc.)
- [ ] Math functions (ABS, ROUND, etc.)
- [ ] Date/time functions
- [ ] `CASE` expressions

### Joins

- [x] `INNER JOIN`
- [ ] `LEFT JOIN`
- [ ] `RIGHT JOIN`
- [ ] `FULL OUTER JOIN`
- [ ] `CROSS JOIN`
- [ ] Self-joins
- [ ] Multi-table joins (3+ tables)

### Aggregation

- [ ] `COUNT(*)`
- [ ] `COUNT(column)`
- [ ] `SUM`
- [ ] `AVG`
- [ ] `MIN`
- [ ] `MAX`
- [ ] `GROUP BY`
- [ ] `HAVING`
- [ ] Aggregation with NULL handling

### Subqueries

- [ ] Scalar subqueries
- [ ] `IN` subqueries
- [ ] `EXISTS` subqueries
- [ ] Correlated subqueries
- [ ] Common Table Expressions (CTE / WITH)

### Execution

- [x] Predicate recheck in IndexScan
- [x] Correct NULL semantics
- [x] Type checking at execution time
- [ ] Proper error messages with line/column info
- [ ] Query cancellation support

### Testing & Debugging

- [x] Debug logging system (multi-level)
- [x] Phase tracking
- [x] Component-based logging
- [ ] Query profiler
- [ ] Memory leak detection
- [ ] Comprehensive test suite for all SQL features

---

## Phase 1.5 — Polish & Completeness

### Parser Improvements

- [x] Position tracking for errors
- [x] Multi-statement support
- [ ] Better error messages (show SQL context)
- [ ] Support for comments in SQL
- [ ] Case-insensitive keywords enforcement
- [ ] Escaped identifiers (quoted column names)
- [ ] Semicolon handling edge cases

### Binder Improvements

- [ ] Better type inference
- [ ] Ambiguous column name detection
- [ ] Table alias validation
- [ ] Column existence validation
- [ ] Type compatibility checking
- [ ] Proper NULL type handling

### Type System

- [ ] Add FLOAT/DOUBLE support
- [ ] Add DATE/TIME/TIMESTAMP types
- [ ] Add BLOB/BYTEA type
- [ ] Type coercion rules (INT → FLOAT, etc.)
- [ ] String encoding (UTF-8 validation)
- [ ] Type casting functions

### Execution Engine Polish

- [ ] Better error propagation in operators
- [ ] Resource cleanup on errors
- [ ] Proper operator lifecycle (open/next/close)
- [ ] Memory-efficient large result sets
- [ ] Query timeout support

### Storage Improvements

- [ ] Variable-length string storage
- [ ] NULL bitmap in rows
- [ ] Row size validation
- [ ] Page overflow handling
- [ ] Tuple visibility (prepare for MVCC)

### Index Improvements

- [ ] Composite indexes (multi-column)
- [ ] Index-only scans (covering indexes)
- [ ] Unique constraint enforcement via index
- [ ] NULL handling in indexes
- [ ] Index rebuild operation

### Catalog Improvements

- [ ] Schema versioning
- [ ] System tables (metadata queries)
- [ ] Information schema views
- [ ] Catalog validation on startup
- [ ] Constraint metadata storage

---

## Phase 2 — Durability & Recovery (CRITICAL)

### Persistence

- [ ] Design stable catalog format
- [ ] Serialize table metadata to disk
- [ ] Serialize index metadata to disk
- [ ] Catalog file format (protobuf/flatbuffers?)
- [ ] Reload catalog on startup
- [ ] Validate catalog integrity on open
- [ ] Catalog upgrade/migration system

### Write-Ahead Logging (WAL)

- [ ] WAL file format design
- [ ] Log record types:
  - [ ] Insert record
  - [ ] Delete record
  - [ ] Update record (before/after)
  - [ ] Checkpoint record
  - [ ] Commit record
- [ ] Log sequence numbers (LSN)
- [ ] Write log before data page
- [ ] WAL writer (fsync discipline)
- [ ] WAL replay on startup
- [ ] WAL truncation after checkpoint
- [ ] WAL circular buffer management

### Buffer Pool Enhancements

- [ ] Dirty page tracking (per-page flag)
- [ ] Page flush discipline (WAL protocol)
- [ ] Eviction policy (LRU)
- [ ] Pin count management
- [ ] Buffer pool statistics
- [ ] Background flusher thread
- [ ] Force page flush before eviction

### Checkpointing

- [ ] Periodic checkpoint trigger
- [ ] Checkpoint coordinator
- [ ] Flush all dirty pages
- [ ] Write checkpoint record to WAL
- [ ] Truncate old WAL segments

### Crash Recovery

- [ ] Redo phase (replay WAL)
- [ ] Undo phase (rollback incomplete txns)
- [ ] Recovery manager
- [ ] Corruption detection
- [ ] Recovery test suite (kill -9 tests)

**Outcome:** ✔ Crash-safe, restartable, durable

---

## Phase 3 — Transactions (ACID)

### Transaction API

- [ ] `BEGIN` / `START TRANSACTION`
- [ ] `COMMIT`
- [ ] `ROLLBACK`
- [ ] Transaction ID (XID) generation
- [ ] Transaction context per connection
- [ ] Auto-commit mode

### Atomicity

- [ ] All-or-nothing writes
- [ ] Rollback via undo log
- [ ] Transaction abort handling
- [ ] Savepoints (optional)

### Isolation (Pick ONE initially)

- [ ] **Option A:** Global write lock (simplest)
- [ ] **Option B:** Page-level locking
  - [ ] Lock manager
  - [ ] Lock table
  - [ ] Deadlock detection
- [ ] **Option C:** MVCC (advanced, but best)
  - [ ] Versioned rows
  - [ ] Transaction snapshots
  - [ ] Visibility checks
  - [ ] Old version cleanup (vacuum)

### Consistency

- [ ] Constraint checking (PRIMARY KEY, UNIQUE, FOREIGN KEY)
- [ ] Trigger support (optional)

### Durability

- [ ] WAL flush on commit
- [ ] Checkpoint integration
- [ ] Durable commit guarantees

**Outcome:** ✔ ACID transactions, production-ready

---

## Phase 4 — Query Power (Advanced SQL)

### Advanced Joins

- [ ] Hash join
- [ ] Sort-merge join
- [ ] Index nested loop join (optimized)
- [ ] Join reordering optimization

### Aggregation

- [x] COUNT, SUM, AVG, MIN, MAX (basic)
- [ ] Hash-based aggregation
- [ ] Sort-based aggregation
- [ ] Streaming aggregation
- [ ] Multiple aggregates per query
- [ ] DISTINCT aggregates

### Window Functions (Advanced)

- [ ] ROW_NUMBER()
- [ ] RANK(), DENSE_RANK()
- [ ] LAG(), LEAD()
- [ ] Partition by support
- [ ] Window frame clauses

### Set Operations

- [ ] UNION
- [ ] UNION ALL
- [ ] INTERSECT
- [ ] EXCEPT

### Query Optimization (Advanced)

- [ ] Join order optimization (dynamic programming)
- [ ] Index vs seq scan cost model
- [ ] Statistics collection (histograms)
- [ ] Cardinality estimation
- [ ] Plan caching

**Outcome:** ✔ Feature-complete relational DB

---

## Phase 5 — Performance & Scale

### Storage Optimization

- [ ] Slotted pages (variable-length records)
- [ ] Free space map
- [ ] Page compaction
- [ ] HOT updates (heap-only tuples)
- [ ] TOAST (large attribute storage)
- [ ] Column-oriented storage (optional)

### Index Optimization

- [ ] Bulk loading (bottom-up build)
- [ ] Index compression
- [ ] Prefix truncation
- [ ] Bloom filters for range scans

### Execution Optimization

- [ ] Vectorized execution
- [ ] JIT compilation (LLVM)
- [ ] Operator fusion
- [ ] Parallel query execution
- [ ] Pipelining vs materialization

### Memory Management

- [ ] Memory-limited hash joins
- [ ] External sorting (disk spill)
- [ ] Work memory management
- [ ] Query memory budgets

### Statistics & Monitoring

- [ ] Table statistics (row count, size)
- [ ] Index statistics (depth, pages)
- [ ] Query statistics (execution time, cache hits)
- [ ] System views (pg_stat_user_tables equivalent)

**Outcome:** ✔ High-performance DBMS

---

## Phase 6 — Production Hardening

### Testing & Validation

- [ ] Property-based testing (proptest)
- [ ] Fuzz testing (cargo-fuzz)
- [ ] Crash testing (kill -9 during writes)
- [ ] Concurrency testing (loom)
- [ ] SQL conformance tests (sqllogictest)
- [ ] Benchmark suite (TPC-H subset)

### Safety & Correctness

- [ ] Checksums on pages
- [ ] Corruption detection
- [ ] Graceful degradation
- [ ] Error recovery paths
- [ ] Memory safety audits
- [ ] No unsafe code (or minimal, audited)

### Observability

- [x] EXPLAIN (basic)
- [ ] EXPLAIN ANALYZE (with timing)
- [ ] Query plan visualization
- [ ] Slow query log
- [ ] Performance counters
- [ ] Trace logging (OpenTelemetry?)

### Tooling

- [x] CLI client (basic)
- [ ] TUI client
- [ ] Interactive REPL
- [ ] Pretty-printed output
- [ ] CSV import/export
- [ ] Backup tool (`helium-backup`)
- [ ] Restore tool (`helium-restore`)
- [ ] Database inspection tool (`helium-inspect`)
- [ ] Schema diff tool

### Documentation

- [ ] User guide
- [ ] SQL reference
- [ ] Architecture docs
- [ ] Performance tuning guide
- [ ] Migration guide (from SQLite)

**Outcome:** ✔ Production-ready, deployable

---

## Phase 7 — Helium-Specific Features (Differentiation)

> These are features that make Helium unique and align with your vision

### Native Query Language (NQL)

- [ ] Design NQL syntax (ORM-free, type-safe)
- [ ] NQL parser
- [ ] NQL → Logical IR compiler
- [ ] Language bindings design (FFI)
- [ ] Rust NQL API
- [ ] Python NQL adapter
- [ ] Go NQL adapter
- [ ] Type-safe query builder API
- [ ] Query composition (reusable query fragments)

### Logical IR (Language-Agnostic Core)

- [ ] Stable IR format (protobuf/flatbuffers)
- [ ] IR validation
- [ ] IR serialization/deserialization
- [ ] IR optimizer passes (independent of frontend)
- [ ] IR versioning

### Multi-Language Support

- [ ] C API (stable ABI)
- [ ] Rust native API
- [ ] Python bindings (via PyO3)
- [ ] Go bindings (via CGO)
- [ ] JavaScript/WASM bindings
- [ ] Language adapter guide (for other languages)

### ORM-Free Philosophy

- [ ] Direct query composition API
- [ ] No string-based SQL generation
- [ ] Compile-time query validation (Rust)
- [ ] Explicit query plans (no magic)
- [ ] Query inspection API (see generated plan)

### Analytics Features (DuckDB-inspired)

- [ ] Columnar execution mode
- [ ] Parquet file import/export
- [ ] Arrow format support
- [ ] Vectorized aggregation
- [ ] Analytical functions (percentile, etc.)
- [ ] Approximate queries (sampling)

### Embedded Optimizations

- [ ] Zero-copy queries (via mmap)
- [ ] Snapshot isolation (for read queries)
- [ ] Read-only mode (for analysis)
- [ ] In-memory database mode
- [ ] WAL-disabled mode (performance vs durability)

### Developer Experience

- [ ] Query debugger (step-through execution)
- [ ] Visual query plan generator
- [ ] Performance profiler (flame graphs)
- [ ] Schema migration framework
- [ ] Seed data support

**Outcome:** ✔ Unique, differentiated DBMS

---

## Phase 8 — Polish & Advanced Features (Long-term)

### Advanced SQL Features

- [ ] Recursive CTEs
- [ ] Lateral joins
- [ ] Array types
- [ ] JSON support (JSON type, operators, functions)
- [ ] Full-text search
- [ ] Regular expressions
- [ ] User-defined functions (UDF)
- [ ] Stored procedures

### Replication (Single-node → Multi-node)

- [ ] Write-ahead log shipping
- [ ] Logical replication
- [ ] Read replicas
- [ ] Point-in-time recovery (PITR)

### Extensions

- [ ] Extension API
- [ ] Custom types
- [ ] Custom operators
- [ ] Custom indexes (GiST, GIN, etc.)

### Security

- [ ] Authentication
- [ ] Role-based access control (RBAC)
- [ ] Row-level security
- [ ] Audit logging
- [ ] Encryption at rest

**Outcome:** ✔ Advanced, enterprise-grade DBMS

---

## Testing Strategy

### Unit Tests

- [x] B+Tree operations
- [x] Buffer pool
- [x] Parser
- [x] Binder
- [x] Optimizer passes
- [ ] WAL replay
- [ ] Transaction isolation

### Integration Tests

- [x] End-to-end SQL queries
- [ ] Multi-statement transactions
- [ ] Crash recovery scenarios
- [ ] Concurrent access

### Performance Tests

- [ ] Insertion benchmarks
- [ ] Query benchmarks
- [ ] Index scan vs seq scan
- [ ] Join performance
- [ ] Large dataset tests (millions of rows)

### Correctness Tests

- [ ] SQL conformance suite
- [ ] Edge case tests (NULL, empty tables, etc.)
- [ ] Data corruption detection
- [ ] Invariant validation

---

## Immediate Next Steps (Phase 1 Completion)

1. **Fix remaining Phase 1 issues:**

   - [ ] Fix multi-statement parsing edge cases
   - [ ] Add OFFSET support
   - [ ] Test INSERT ... SELECT
   - [ ] Implement LEFT JOIN
   - [ ] Add DISTINCT

2. **Complete Phase 1.5 (Polish):**

   - [ ] Improve error messages (show SQL context, line/column)
   - [ ] Add missing SQL operators (IN, BETWEEN, LIKE)
   - [ ] Implement basic aggregation (COUNT, SUM, etc.)
   - [ ] Add composite index support
   - [ ] Write comprehensive test suite

3. **Begin Phase 2 (Durability):**
   - [ ] Design catalog persistence format
   - [ ] Implement catalog serialization
   - [ ] Design WAL format
   - [ ] Implement basic WAL writer
   - [ ] Test crash recovery

---

## Success Metrics

### Phase 1 Complete When:

- [ ] All basic SQL DML/DDL works
- [ ] Joins work correctly
- [ ] Basic aggregation works
- [ ] 100+ integration tests pass
- [ ] No known correctness bugs

### Phase 2 Complete When:

- [ ] Database survives kill -9
- [ ] WAL replay works correctly
- [ ] Catalog persists across restarts
- [ ] Fuzz testing passes (1M iterations)

### Phase 3 Complete When:

- [ ] BEGIN/COMMIT/ROLLBACK work
- [ ] Isolation prevents dirty reads
- [ ] Deadlock detection works
- [ ] TPC-C subset passes

### Production-Ready When:

- [ ] Phases 1-3 complete
- [ ] 10K+ test cases pass
- [ ] No memory leaks
- [ ] No data corruption in fuzz tests
- [ ] Performance competitive with SQLite

### Helium 1.0 When:

- [ ] NQL implemented
- [ ] Multi-language support
- [ ] Documentation complete
- [ ] Real-world deployments

---

## Notes

- **Priorities:** Durability (Phase 2) is the biggest jump to production
- **Trade-offs:** Start with simple locking, migrate to MVCC later
- **Testing:** Write tests for every feature before moving on
- **Performance:** Don't optimize prematurely, but measure everything
- **NQL:** Design it early, implement after core is stable

**Remember:** A database that works is better than one with features.

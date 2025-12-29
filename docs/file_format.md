# File Format (End-State Design)

This document describes the _target_ on-disk storage format for Helium.

The initial implementation may be simpler, but must evolve toward this model.

---

## Storage Model

- Row-oriented storage
- Append-only table heap
- Fixed-size pages (e.g., 4KB)
- Slot directory per page

---

## Page Layout

```
| Page Header |
| Slot Directory |
| Free Space |
| Row Data |
```

- Page Header: metadata, free space pointer
- Slot Directory: offsets to row entries
- Rows stored as variable-length records

---

## Row Encoding

Each row is encoded as:

```
| column_count |
| column_id | type_tag | value |
...
```

Supported types:

- Int64
- Bool
- String
- Null

---

## Durability

Helium targets durability via:

- Shadow paging (preferred initial approach)
  OR
- Write-ahead logging (future)

The choice is isolated from the execution layer.

---

## Evolution

This format is designed to support:

- Updates
- Deletes
- Indexes
- MVCC

without requiring changes to the query engine.

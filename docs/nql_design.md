# NQL (Native Query Language)

NQL is Helium’s planned native query language.

It is designed to be:

- Type-safe
- Composable
- Embedded-friendly
- Faster than SQL parsing

---

## Goals

- Express queries as code, not strings
- Compile directly to Logical IR
- Avoid runtime SQL parsing costs
- Preserve optimizer benefits

---

## Example

```rust
db.select(users)
  .filter(users.age.gt(18))
  .project(|u| (u.name, u.city))
  .order_by(users.age.desc())
  .limit(10)
```

## Design Principles

- Explicit expressions
- No string-based column access
- Compile-time validation where possible
- Same optimizer and execution pipeline as SQL

## Relationship to SQL

- SQL and NQL produce identical Logical IR
- Optimizer treats them identically
- NQL is an alternative frontend, not a replacement

## Status

NQL is a post-core feature.
The query engine must be fully stable before NQL is implemented.

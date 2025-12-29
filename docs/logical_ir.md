# Logical IR (Intermediate Representation)

The Logical IR represents queries after parsing and binding, but before
execution decisions are made.

It is a tree of immutable operators describing _what_ to compute, not _how_.

---

## Logical Operators

### Scan

Represents a table scan.

```
Scan(table)
```

### Filter

Filters rows using a predicate expression.

```
Filter(predicate)
└─ input
```

### Project

Computes output expressions and assigns column aliases.

```
Project [(expr, alias)...]
└─ input
```

### Join

Represents an inner join with an explicit join condition.

```
Join(on)
├─ left
└─ right
```

### Sort

Orders rows using one or more keys.

```
Sort [(expr, asc)...]
└─ input
```

### Limit

Restricts the number of output rows.

```
Limit n
└─ input
```

---

## Expressions

Expressions are pure and side-effect free.

- BoundColumn(table, name)
- Literal(value)
- Unary(op, expr)
- Binary(left, op, right)

Unbound column references are **not allowed** in the Logical IR.

---

## Properties

- Fully bound
- Deterministic
- Immutable
- Storage-independent

The Logical IR is the primary input to the optimizer.

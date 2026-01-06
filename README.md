# Helium

**Helium** is a lightweight, embeddable database engine designed to be
**robust, safe, and easy to use**, while giving developers **direct control**
over how queries are expressed and executed.

Think of Helium as:

> **SQLite-level embeddability**
>
> - **DuckDB-style analytics**
> - **ORM-free, language-native querying**

---

## Why Helium?

Most databases force you into one of two bad choices:

- Write raw SQL strings everywhere
- Use heavy ORMs that hide behavior, add overhead, and break abstractions

Helium takes a different path.

### Helium is built to be:

- **Robust** – designed to never break under edge cases or unexpected usage
- **Embeddable** – link it directly into your application like SQLite
- **Safe** – strong guarantees, predictable behavior, no hidden magic
- **Easy** – simple mental model, minimal configuration

---

## Key Features

- Embedded database (single-node, in-process)
- SQL support for familiar workflows
- Planned **Native Query Language (NQL)** for ORM-free development
- Deterministic and predictable query behavior
- Designed for long-term stability and correctness

---

## No ORMs — By Design

Helium is built on the belief that **ORMs are fundamentally inefficient and leaky**.

Problems with traditional ORMs:

- Implicit queries
- Poor performance visibility
- Complex object-to-table mapping
- Hard-to-debug query generation
- Loss of control over execution

### Helium’s approach

Instead of ORMs, Helium introduces a **language-independent Logical IR (LIR)**.

- Queries are expressed directly as structured operations
- No string-based SQL generation
- No runtime query guessing
- No hidden joins or N+1 traps

Your application talks **directly** to Helium’s query core.

---

## Native Query Language (NQL)

NQL is Helium’s planned alternative to SQL and ORMs.

It is designed to feel like an **object-oriented, type-safe query language**
that can integrate naturally with any backend language.

Example (conceptual):

```rust
db.select(users)
  .filter(users.age > 18)
  .project(users.name, users.city)
  .limit(10)
```

### What makes NQL different?

- No strings
- No reflection
- No runtime SQL parsing
- No ORM layers
- Full control over query shape

NQL compiles directly into Helium’s Logical IR.

---

## Can NQL Work Across Multiple Languages?

Yes — **and this is a core design goal.**

Helium is designed so that:

- The **Logical IR is language-agnostic**
- Each language can have a thin adapter that:
  - Converts native language constructs → LIR
  - Sends LIR directly to Helium
- No ORM layer is required

This means:

- Rust, Go, Python, Java, etc. can all share the same query core
- Each language keeps its own idiomatic API
- Performance stays predictable and transparent

In short:

> **NQL is not tied to one language — it’s a portable query model.**

---

## SQL Support

Helium supports SQL for:

- Familiar workflows
- Debugging
- Tooling
- Quick prototyping

SQL and NQL are **equals**:

- Both produce the same internal query representation
- Both use the same optimizer
- Both use the same execution engine

NQL is an alternative frontend, not a replacement.

---

## Philosophy

Helium follows a few strict principles:

### 1. Robustness Over Features

Helium prioritizes:

- Correctness
- Stability
- Predictable behavior

A smaller feature set that never breaks is better than a large one that does.

---

### 2. Never Break User Data

Helium is built with the mindset that:

> _A database failing is not an option._

Design decisions favor:

- Strong invariants
- Defensive checks
- Clear failure modes
- No undefined behavior

---

### 3. Safe and Easy by Default

- No footguns
- No hidden behavior
- No surprising performance cliffs
- Clear boundaries between user code and the database

If something goes wrong, it should be obvious **why**.

---

### 4. ORMs Are a Dead End

Helium rejects the idea that databases should adapt to object models.

Instead:

- Applications adapt to **queries**
- Queries adapt to **data**
- The database stays simple, fast, and honest

---

## Who Is Helium For?

Helium is a good fit if you want:

- An embedded database like SQLite
- Strong guarantees and predictability
- Full control over queries
- ORM-free data access
- A database that grows with your application

Helium is **not** trying to be:

- A distributed database
- A cloud service
- A full DBMS system with all bells and whistles

---

## Status

Helium is actively developed with a **correct, stable core** and a clear roadmap.

NQL and multi-language adapters are planned after the core reaches full stability.

---

## Final Thought

> **Helium is a database you embed, trust, and forget about —
> not one you fight with.**

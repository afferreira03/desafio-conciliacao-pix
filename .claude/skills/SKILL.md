---
name: pix-hexagonal-conventions
description: Use when implementing, structuring, or reviewing code in the reconciliation module — package placement, layering rules, domain-event handling, or where a new class should live.
---

# Hexagonal / DDD conventions for this project

## Package structure (Spring Modulith)

- Base package: `br.com.desafio.conciliacaopix`.
- Business modules are **direct subpackages** of the base package
  (e.g. `reconciliation`) — Spring Modulith derives module boundaries from
  this, so nothing should sit as a direct sibling that isn't meant to be
  its own module.
- Within a module, the standard hexagonal split:
  - `domain/` — model, value objects (`vo/`), domain events (`event/`),
    domain services. **Zero framework imports allowed here** — no Spring,
    no Mongo, no Jackson annotations. This is the module's most valuable
    property; don't compromise it for convenience.
  - `application/` — use-case services, and `port/in` / `port/out`
    interfaces (the hexagon's ports).
  - `infrastructure/` — `adapter/in` (e.g. Kafka consumers),
    `adapter/out` (e.g. Mongo adapters, Kafka producers), and `config/`.
- Nesting deeper inside `infrastructure` does **not** create new Modulith
  modules — only first-level subpackages of the base package do. Organize
  freely below that level.

## Conventions to follow

- **Static `fromDomain(...)` factories** on persistence Document classes
  (see `ReconciliationDocument`, `OutboxEventDocument`) to map a domain
  object into its storage shape. Since static methods can't receive
  Spring-injected beans, anything requiring a bean (e.g. JSON
  serialization via `ObjectMapper`) must be done by the **caller** and
  passed in as a plain value — not inside the factory method.
- **Sealed `DomainEvent` hierarchy**: when code needs to branch on which
  concrete event type it has, use an exhaustive `switch` pattern match
  over the sealed type (Java 21+, JEP 441) — never `instanceof` chains
  and never `event.getClass()`/reflection. The whole point of `sealed` is
  that the compiler forces every switch to be updated when a new event
  type is added; reflection-based branching silently defeats that.
- **Value Objects are records** (e.g. `EndToEndId(String value)`) with
  validation in a compact constructor. Access the raw value via the
  accessor (`.value()`), never assume a public field.
- **Avoid duplicated construction logic** across near-identical switch
  branches — if multiple branches build the "same shape" with one
  differing detail (e.g. a label), extract a single shared builder
  method called from each branch, or from one switch that returns
  everything needed in one pass.
- **Mongo indexes**: any field a query filters or sorts by needs an
  index — `@Indexed` for a single field (see `endToEndId` on
  `ReconciliationDocument`), or `@CompoundIndex` at the class level when
  a query both filters on one field and sorts on another (e.g. outbox
  polling by `status` + `createdAt`).
- **Atomicity**: when two documents must be written together as one
  unit (e.g. domain state + outbox event), do it inside the same
  `@Transactional` method on the same adapter — don't split it across
  two separate calls or two separate adapters.

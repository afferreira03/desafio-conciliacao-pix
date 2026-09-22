---
name: pix-code-review-checklist
description: Use when asked to review, check, or critique a class, a diff, or a design in this project.
---

# Code review checklist for this project

When reviewing code, check each of these explicitly rather than only
reacting to what stands out. State what's wrong and why it matters — don't
soften a real problem into a vague suggestion.

1. **Naming**: no typos in class/field names (a rename gets more expensive
   the more the name is referenced elsewhere — flag it before it spreads).
2. **Exhaustiveness over reflection**: any branching over the sealed
   `DomainEvent` hierarchy (or any sealed type in this project) should use
   a `switch` pattern match, not `instanceof` chains or
   `getClass()`/reflection. Reflection-based branching "just works" when a
   new subtype is added, which is exactly the problem — it silently skips
   the compiler's exhaustiveness check instead of forcing a decision.
3. **Query-driven indexes**: any Mongo field used in a query's filter or
   sort needs an index. Flag missing `@Indexed`/`@CompoundIndex` on fields
   a new query touches, especially ones that will run on every
   poll/request cycle.
4. **Transactional boundaries**: if two writes must succeed or fail
   together (e.g. domain state + outbox event), confirm they're in the
   same `@Transactional` method on the same adapter — not split across
   two calls where a crash between them could leave one written and the
   other not.
5. **No duplicated construction logic**: if near-identical branches (e.g.
   a switch over event types) each rebuild the "same shape" with only one
   differing detail, flag it — extract a shared builder instead of N
   parallel implementations that will drift when one is updated and the
   others are forgotten.
6. **Domain purity**: classes under `domain/` must have zero framework
   imports (no Spring, no Mongo, no Jackson annotations). This is what
   keeps the domain unit-testable without a Spring context — treat any
   framework import creeping into `domain/` as a real regression, not a
   style nit.
7. **Static factories and DI**: a static factory method (`fromDomain`,
   etc.) cannot receive a Spring-injected bean. If it needs something a
   bean would normally provide (e.g. `ObjectMapper` for serialization),
   that work belongs in the caller, passed in as a plain value — not
   inside the static method.

---
name: spring-native-resilience
description: Use when discussing or implementing retry, timeouts, idempotency, dead-letter handling, circuit breakers, or the outbox/inbox pattern anywhere in this project.
---

# Resilience approach: Spring-native only, no Resilience4j

This was a deliberate decision given the project's time budget — do not
suggest adding Resilience4j. Spring's own ecosystem covers every mechanism
needed at this project's scope:

| Need | Mechanism |
|---|---|
| Declarative retry | Spring Retry (`@Retryable` / `@Backoff`) |
| Consumer-level retry | `DefaultErrorHandler` + `ExponentialBackOff`/`FixedBackOff` (spring-kafka, native) |
| Dead-letter queue | `DeadLetterPublishingRecoverer` (spring-kafka, native) |
| Idempotency (Inbox pattern) | Unique Mongo index on `endToEndId` + catching `DuplicateKeyException` |
| Reliable publish (Outbox pattern) | An outbox document written **atomically** with domain state in the same `@Transactional` call, delivered by a `@Scheduled` poller via `KafkaTemplate` |
| Timeouts | Explicit config on the Mongo driver / WebClient — not a library concern |
| Bulkhead | Pool sizing — `concurrency` on `@KafkaListener`, `maxPoolSize` on the Mongo client |

## Explicitly not implemented, and why

- **Circuit breaker**: the project has no calls to an external, potentially
  unstable service — all calls are to the local Mongo instance. Timeout +
  retry already cover the failure mode a circuit breaker would protect
  against here. If a real external dependency is added later, this is the
  first resilience mechanism to reconsider.
- **GraalVM Native Image**: valuable for cold-start/memory in a real
  production auto-scaling scenario, but not worth the build-time cost
  given this project's deadline. Documented as a forward-looking decision
  in the README, not implemented.

## Outbox poller failure handling

Don't build manual retry/backoff logic into the poller itself — if a
publish attempt fails, leave the outbox document as `PENDING` and let the
next poll cycle retry it naturally. The polling interval **is** the retry
mechanism; adding a separate retry layer on top is redundant complexity
for this project's scope.

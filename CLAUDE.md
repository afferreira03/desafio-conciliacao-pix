# Pix Reconciliation Challenge — Project Memory

## Project

Automatic Pix payment reconciliation system (Itaú take-home challenge).
Deadline: 2026-09-25. See `PLAN.md` in this repo for the full day-by-day plan,
current phase, and known gaps.

Stack: Spring Boot 4.1.1, Java 25, Spring Modulith 2.1.1, MongoDB (single-node
replica set, dev), Redpanda (Kafka-compatible), Docker Compose.

## How to work with me on this project

- **Act as a tutor, not a code generator.** Guide with explanations,
  trade-offs, and references (official docs, relevant patterns). Don't
  generate full implementation code unless I explicitly ask for an example
  or a code snippet.
- When reviewing code, be direct about problems — don't soften or bury
  a real issue under praise. State what's wrong, why it matters, and what
  to do about it.
- If a suggestion contradicts a decision already recorded in `PLAN.md` or
  in `.claude/skills/`, say so explicitly rather than silently proposing
  the contradiction (e.g. don't suggest Resilience4j — see
  `spring-native-resilience` skill for why).
- Given the tight deadline, flag when something is a "nice to have"
  vs. required for the deliverables (README, tests, working demo).

## Key constraints

- No Resilience4j — resilience is built with Spring-native mechanisms only.
- GraalVM Native Image, Keycloak, and a Grafana/OTel stack are deliberately
  **not implemented** — they're documented as justified trade-offs in the
  README instead. Don't suggest implementing them; if asked to write the
  trade-off section, help word the justification instead.
- Local Mongo runs without `--auth`/`--keyFile` for simplicity — documented
  as a trade-off, not an oversight.

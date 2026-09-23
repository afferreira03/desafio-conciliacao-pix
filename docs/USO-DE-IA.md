# Uso de IA no desenvolvimento

> **Rascunho** — preparado com apoio da própria IA a partir do histórico das sessões; **revisar e completar** (itens
> `TODO`) antes da entrega. O objetivo é deixar explícito *como* e *em que momento* a IA foi usada, e o que foi decisão
> e trabalho próprio.

**Ferramenta**: Claude (Anthropic) — conversas de arquitetura e, na reta final, Claude Code (CLI) com acesso ao
repositório. `TODO` citar outras ferramentas usadas (ex.: assistente da IDE), se houver.

**Modo de trabalho combinado**: na maior parte do projeto a IA atuou como **tutora/revisora** — explicações,
referências e revisão de código, com a implementação feita por mim. As exceções (código gerado pela IA) estão
listadas explicitamente abaixo e foram revisadas por mim.

---

## 1. Concepção e arquitetura (sessões iniciais)

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Leitura do case | Discussão dos NFRs e do que priorizar num prazo de 7 dias | `TODO` |
| Stack inicial | Crítica da proposta (Spring Boot + GraalVM + Keycloak + Mongo + Kafka): recomendação de tratar GraalVM e Keycloak como trade-offs documentados | Decisão de não implementar GraalVM/Keycloak/Grafana e documentá-los |
| Padrões de mensageria | Correção conceitual: **Outbox** protege a *publicação*; **Inbox / Idempotent Consumer** protege o *consumo* | Adoção dos dois padrões |
| Estilo arquitetural | Sugestão de monólito modular + hexagonal | `TODO` |
| Resiliência | Mapeamento de mecanismos nativos do Spring equivalentes ao Resilience4j (retry, DLQ, idempotência, timeouts) | Decisão de não usar Resilience4j |
| Modelagem DDD | Discussão de agregado, value objects e eventos de domínio | `TODO` |

## 2. Infraestrutura local

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Docker Compose do MongoDB | Diagnóstico de falhas de inicialização: `MONGO_INITDB_ROOT_*` ativa `--auth`, e replica set + auth exige `--keyFile` | Remoção da autenticação local, documentada como trade-off; serviço `mongo-init` idempotente |
| Redpanda Console | Correção da imagem do console | `TODO` |

## 3. Implementação e revisão de código

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Domínio, serviço de aplicação, adapters Mongo/Kafka, outbox | Revisões de código e explicações; implementação por mim | `TODO` |
| Revisão pós-outbox (22/09) | A revisão apontou: (1) `DuplicateKeyException` capturada **dentro** da transação — no Mongo o erro aborta a transação, então duplicatas iriam para a DLQ; (2) corrida entre dois pagamentos da mesma fatura; (3) pagamento com valor divergente podia **reverter** fatura paga para `ABERTA`; (4) `expectedAmount` perdido nos eventos | Correções implementadas por mim: verificação prévia de idempotência, update condicional (compare-and-set), `updatedInvoice` só em transição real; iterações revisadas pela IA até ficarem corretas |
| Testes unitários adicionais | **Gerados pela IA a meu pedido** (motor, `Invoice`, serviço de conciliação; depois relay do outbox, compare-and-set, value objects, mapeamentos) | Revisão e execução |
| API REST + OpenAPI | Planejamento dos endpoints com a IA; **implementação feita pela IA a meu pedido** (prazo), com minha revisão | Escopo e prioridades dos endpoints; verificação manual no ambiente |
| Bugs encontrados durante a API | IA identificou `toDomain` quebrando em registros sem `txId`/`expectedAmount` e índice de fallback apontando para campo inexistente | Validação no ambiente |
| Preparação da carga | Desenho do gerador de carga e do mix de cenários; limite de lote no outbox | `TODO` |

## 4. Documentação

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| README | Esqueleto gerado pela IA a partir das decisões já tomadas | `TODO` redação final e revisão |
| Este documento | Rascunho gerado pela IA | `TODO` revisão |

---

## O que a IA **não** fez / limites do uso

- `TODO` decisões tomadas sem apoio da IA.
- Toda sugestão foi validada: testes executados localmente e verificação manual no ambiente Docker.
- Casos em que a sugestão da IA foi rejeitada ou corrigida: `TODO` (ex.: sugestões que contradiziam decisões já tomadas).

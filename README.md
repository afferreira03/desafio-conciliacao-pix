# Conciliação Automática de Pagamentos Pix

> Case técnico — Engenharia de Software Backend (Itaú).
> **Status do documento: esqueleto.** Seções marcadas com `TODO` ainda precisam ser completadas/revisadas.

Sistema que consome transações Pix em tempo real (via Kafka/Redpanda), associa cada uma a uma fatura existente e
classifica o resultado como **CONCILIADO**, **PENDENTE** ou **INCONSISTENTE**, publicando o resultado como evento e
expondo relatórios via API REST.

---

## Sumário

1. [Visão geral da solução](#1-visão-geral-da-solução)
2. [Arquitetura](#2-arquitetura)
3. [Regras de conciliação](#3-regras-de-conciliação)
4. [Como executar](#4-como-executar)
5. [API REST](#5-api-rest)
6. [Mensageria e contratos de eventos](#6-mensageria-e-contratos-de-eventos)
7. [Resiliência e confiabilidade](#7-resiliência-e-confiabilidade)
8. [Escalabilidade, latência e vazão](#8-escalabilidade-latência-e-vazão)
9. [Segurança e LGPD](#9-segurança-e-lgpd)
10. [Observabilidade](#10-observabilidade)
11. [Estratégia de testes](#11-estratégia-de-testes)
12. [Decisões técnicas e trade-offs](#12-decisões-técnicas-e-trade-offs)
13. [Limitações e evoluções](#13-limitações-e-evoluções)
14. [Uso de IA](#14-uso-de-ia)

---

## 1. Visão geral da solução

| Requisito do case | Como é atendido |
|---|---|
| Consumir fluxo de transações Pix em tempo real | Consumer Kafka (`pix.transactions`), 6 partições / 6 threads |
| Associar transação a pedido/fatura | Motor de conciliação de domínio: por `txId`; fallback por chave Pix + valor + janela de tempo |
| Detectar e reportar não conciliadas/divergentes | Status `PENDENTE` / `INCONSISTENTE` com motivo; evento em `pix.reconciliation.result`; API de consulta |
| Relatórios simples por status | `GET /api/v1/reconciliations/summary` e listagem filtrável |
| Escalável e tolerante a falhas | Particionamento Kafka, consumidor idempotente, outbox transacional, retry + DLQ |

**Stack**: Java 25 · Spring Boot 4.1 · Spring Modulith 2.1 · MongoDB 7 (replica set) · Redpanda (API Kafka) ·
springdoc-openapi · Docker Compose.

---

## 2. Arquitetura

**Monólito modular + arquitetura hexagonal (ports & adapters).** O domínio (`domain/`) não conhece Spring, Mongo nem
Kafka; a aplicação (`application/`) orquestra casos de uso via portas; a infraestrutura (`infrastructure/`) implementa
os adapters de entrada (Kafka, REST) e saída (Mongo, Kafka).

```
reconciliation/
├── domain/          # ReconciliationEngine, Invoice, ReconciliationRecord, value objects, eventos (sealed)
├── application/     # casos de uso (port/in), portas de saída (port/out), serviços, modelos de leitura
└── infrastructure/
    ├── in/          # Kafka consumer, REST controllers, OpenAPI, tratamento de erros (ProblemDetail)
    └── adapter/out/ # Mongo (persistência, consultas, outbox), Kafka (relay do outbox)
```

`TODO` Diagrama de componentes/módulos (gerado com `spring-modulith-docs`).

`TODO` Diagrama de sequência do fluxo de conciliação (consumer → serviço → motor → transação Mongo → outbox → relay → tópico de resultado).

---

## 3. Regras de conciliação

1. **Idempotência**: se já existe conciliação para o `endToEndId`, retorna o registro existente (nada é reprocessado).
2. **Com `txId`**: busca a fatura pelo `txId`.
3. **Sem `txId`**: fallback por **chave Pix + valor + janela de ±30 min** entre faturas `ABERTA`.
   - 1 candidata → segue para o motor; mais de 1 → `INCONSISTENTE / MULTIPLE_INVOICES_MATCHED`.
4. **Motor** (`ReconciliationEngine`, domínio puro):

| Situação da fatura | Resultado | Motivo | Efeito na fatura |
|---|---|---|---|
| Não encontrada | `PENDENTE` | — | — |
| Já paga | `INCONSISTENTE` | `INVOICE_ALREADY_PAID` | — |
| Aberta, porém vencida | `INCONSISTENTE` | `INVOICE_EXPIRED` | `ABERTA → EXPIRADA` |
| Já expirada | `INCONSISTENTE` | `INVOICE_EXPIRED` | — |
| Cancelada | `INCONSISTENTE` | `INVOICE_CANCELLED` | — |
| Valor divergente | `INCONSISTENTE` | `AMOUNT_MISMATCH` | — |
| Aberta e valor confere | `CONCILIADO` | — | `ABERTA → PAGA` |

---

## 4. Como executar

**Pré-requisitos**: Docker + Docker Compose, Java 25, Maven 3.9+.

```bash
# 1. Infraestrutura (Redpanda, Redpanda Console, MongoDB em replica set)
docker compose up -d
docker compose ps            # mongo-init deve ter finalizado com exit code 0

# 2. Aplicação
mvn spring-boot:run

# 3. Testes
mvn test
```

| Serviço | URL |
|---|---|
| API | http://localhost:8081/api/v1 |
| Swagger UI | http://localhost:8081/swagger-ui.html |
| OpenAPI (JSON) | http://localhost:8081/v3/api-docs |
| Health / métricas | http://localhost:8081/actuator/health · `/actuator/metrics` · `/actuator/prometheus` |
| Redpanda Console | http://localhost:8080 |

`TODO` Passo a passo da demo: criar fatura → publicar Pix → consultar resultado; gerador de carga.

---

## 5. API REST

Documentação interativa em `/swagger-ui.html`. Erros no formato **ProblemDetail (RFC 9457)**.

| Método | Endpoint | Descrição |
|---|---|---|
| GET | `/api/v1/reconciliations/{endToEndId}` | Conciliação de uma transação (200 / 400 / 404) |
| GET | `/api/v1/reconciliations?status&reason&from&to&page&size` | Listagem paginada e filtrável (`size` ≤ 100) |
| GET | `/api/v1/reconciliations/summary?from&to` | Relatório: quantidade e valor por status; motivos das inconsistências |
| POST | `/api/v1/invoices` | Abre fatura (201 / 400 / 409) |
| GET | `/api/v1/invoices/{txId}` | Consulta fatura (chave Pix mascarada) |

---

## 6. Mensageria e contratos de eventos

| Tópico | Papel | Partições |
|---|---|---|
| `pix.transactions` | Entrada: transações Pix recebidas | 6 |
| `pix.transactions.DLT` | Dead-letter: mensagens que falharam após as retentativas | 6 |
| `pix.reconciliation.result` | Saída: resultado da conciliação (via outbox), chave = `endToEndId` | 6 |

Exemplo de mensagem de entrada:

```json
{
  "endToEndId": "E0000000020260919123456789012345",
  "txId": "TX123",
  "transactionAmount": 150.00,
  "paymentTimestamp": "2026-09-23T13:45:10Z",
  "pixKey": "user@email.com"
}
```

`TODO` Exemplo de evento de saída (`PixReconciledEvent` / `PixPendingEvent` / `PixInconsistentEvent`) e nota sobre o
contrato: hoje o evento de domínio é serializado diretamente (value objects aparecem como `{"value": ...}`) — evolução:
DTO de integração versionado / schema registry.

---

## 7. Resiliência e confiabilidade

Implementada **somente com mecanismos nativos do ecossistema Spring** (sem Resilience4j — ver trade-offs).

| Falha / risco | Mecanismo | Onde |
|---|---|---|
| Mensagem duplicada (redelivery) | **Idempotent Consumer**: verificação prévia por `endToEndId` + índice único como última barreira | `ReconcilePixTransactionService`, `ReconciliationDocument` |
| Dual-write (Mongo + Kafka) | **Transactional Outbox**: registro + fatura + evento na mesma transação Mongo; relay publica depois | `ReconciliationMongoAdapter`, `OutboxEventPoller` |
| Dois pagamentos concorrentes para a mesma fatura | **Compare-and-set**: update condicionado a `status = ABERTA`; se não casar → rollback + retry → `INVOICE_ALREADY_PAID` | `ReconciliationMongoAdapter` |
| Falha transitória no processamento | Retry do consumer (`DefaultErrorHandler`, 3 tentativas, backoff fixo) | `KafkaConsumerConfig` |
| Mensagem envenenada / irrecuperável | **Dead-letter topic** após as retentativas; erro de desserialização vai direto | `KafkaConsumerConfig` |
| Broker indisponível na publicação do resultado | Outbox permanece `PENDING`; relay reenvia no próximo ciclo (lotes FIFO, timeout explícito) | `OutboxEventPoller` |

Garantia de entrega do resultado: **at-least-once** (consumidores do tópico de resultado devem ser idempotentes por `endToEndId`).

`TODO` Explicar por que o `DuplicateKeyException` não é capturado dentro da transação (erro de escrita aborta a transação Mongo).

---

## 8. Escalabilidade, latência e vazão

NFRs do case: latência ≤ 2 s; 2.000 TPS em média, picos de 7.000 TPS.

`TODO` Resultados do teste de carga (burst de 5.000 transações): taxa de produção, vazão fim-a-fim, latência p50/p95/p99, tabela esperado × obtido.

`TODO` Argumento de escala:
- Paralelismo = partições; escala horizontal adicionando instâncias ao consumer group (até o nº de partições).
- Chave de particionamento por `txId` preserva a ordem por fatura.
- Gargalo principal: transação Mongo por mensagem → evoluções: listener em lote + bulk write, sharding, relay via CDC.

---

## 9. Segurança e LGPD

- **Minimização**: o registro de conciliação não armazena a chave Pix; a API de faturas devolve a chave **mascarada**.
- **Sem autenticação na API nesta versão** — trade-off consciente (ver seção 12). Proposta: OAuth2/OIDC (ex.: Keycloak)
  para a API de relatórios com papéis de leitura; mTLS/SASL entre serviços e broker.
- **MongoDB local sem `--auth`/`--keyFile`** para simplificar o ambiente de desenvolvimento; em produção: autenticação,
  TLS e criptografia em repouso.
- `TODO` Retenção de dados, logs sem dados pessoais, criptografia de campo para a chave Pix.

---

## 10. Observabilidade

- Spring Boot Actuator: `health`, `info`, `metrics`, `prometheus`.
- `TODO` Métricas customizadas (Micrometer): contador por status de conciliação; timer de latência
  (`paymentTimestamp` → conciliação persistida).
- `TODO` Indicadores e alertas propostos: latência p99 > 2 s; lag do consumer group; mensagens no DLT;
  idade do evento `PENDING` mais antigo no outbox; taxa de `INCONSISTENTE` acima do normal; health do Mongo/Kafka.
- Stack Grafana/OpenTelemetry não implementada — ver trade-offs.

---

## 11. Estratégia de testes

| Camada | Tipo | O que cobre |
|---|---|---|
| Domínio | Unitário puro (sem Spring) | Motor de conciliação (todas as regras), `Invoice`, value objects |
| Aplicação | Unitário com Mockito | Orquestração (txId, fallback, ambiguidade, idempotência), relatório, faturas |
| Adapters | Unitário com mocks | Compare-and-set da fatura, relay do outbox (sucesso, falha parcial, timeout, drenagem em lotes), mapeamentos |
| Web | MockMvc standalone | Status HTTP, validação, ProblemDetail, mascaramento |
| Integração | `TODO` Testcontainers (Mongo replica set + Kafka) | Rollback real da transação, DLQ, fluxo ponta a ponta |
| Carga | Gerador de burst | Vazão, latência, contagens esperado × obtido |

Execução: `mvn test` (não requer Docker).

---

## 12. Decisões técnicas e trade-offs

| Decisão | Motivo | Custo / alternativa |
|---|---|---|
| Monólito modular (Spring Modulith) + hexagonal | Entrega viável no prazo com fronteiras claras; domínio testável sem infra | Microsserviços: deploy/escala independentes, mas alto custo operacional para o escopo |
| MongoDB (replica set) | `TODO` justificar (transações multi-documento, esquema flexível, sharding horizontal) | Relacional: `TODO` |
| Redpanda | API Kafka, binário único, leve para dev | Em produção: Kafka gerenciado (ex.: MSK) — código não muda |
| Outbox por polling | Simples, sem infraestrutura extra | CDC (Debezium / change streams) em volumes maiores; múltiplas instâncias do relay publicam duplicados (at-least-once) |
| Idempotência por verificação + índice único | Sem tabela de inbox extra | Inbox dedicada permitiria auditoria de mensagens recebidas |
| Sem Resilience4j | Prazo; os mecanismos nativos cobrem retry, DLQ, timeout, idempotência | Circuit breaker desnecessário: não há dependência externa síncrona instável no escopo |
| Sem Keycloak / controle de acesso | Prazo | Proposta na seção 9 |
| Sem GraalVM Native Image | Custo de configuração (reflection, Kafka/Mongo) × ganho de startup/memória | Evolução para ambientes com scale-to-zero |
| Sem Grafana/OpenTelemetry | Prazo; Actuator + Prometheus endpoint já expõem métricas | Tracing distribuído e dashboards como evolução |
| Demonstração de vazão por burst | Um laptop não representa um cluster; burst medido + argumento de escala | Teste de carga sustentado em ambiente dimensionado |

---

## 13. Limitações e evoluções

- Pix `PENDENTE` não é reprocessado quando a fatura chega depois → job/evento de reconciliação tardia.
- Mensagens no DLT não têm reprocessamento automatizado → ferramenta de replay com correção.
- Registros `SENT` do outbox não são expurgados → índice TTL em `sentAt`.
- Contrato de evento acoplado ao modelo de domínio → DTO de integração versionado / schema registry.
- Nomes de campos de valor diferentes entre eventos (`amount` × `transactionAmount`).
- `TODO` demais limitações identificadas no teste de carga.

---

## 14. Uso de IA

Detalhado em [docs/USO-DE-IA.md](docs/USO-DE-IA.md): em que momentos, para quê e o que foi decisão/implementação própria.

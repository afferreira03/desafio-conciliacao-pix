# Uso de IA no desenvolvimento

> Documento preparado com apoio da própria IA a partir do histórico das sessões e revisado por mim. Itens marcados
> **[confirmar]** dependem de informação que só eu posso validar e devem ser revisados antes da entrega.

**Ferramenta**: Claude (Anthropic) — conversas de arquitetura e revisão ao longo do projeto e, na reta final, Claude
Code (CLI) com acesso ao repositório. **[confirmar]** outras ferramentas usadas (ex.: assistente da IDE), se houver.

**Modo de trabalho combinado**: na maior parte do projeto a IA atuou como **tutora/revisora** — explicações,
referências e revisão de código, com a implementação feita por mim. Nos dois últimos dias, por causa do prazo, pedi
explicitamente que a IA **implementasse** partes do projeto; essas partes estão listadas abaixo como "gerado pela IA
a meu pedido". Em todos os casos, o plano de cada etapa foi aprovado por mim antes da implementação, cada comando
(build, testes, Docker, commits) foi executado só com minha autorização, e o resultado foi revisado por mim.

---

## 1. Concepção e arquitetura (sessões iniciais)

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Leitura do case | Discussão dos NFRs e do que priorizar num prazo de 7 dias | Priorização final do escopo **[confirmar]** |
| Stack inicial | Crítica da proposta (Spring Boot + GraalVM + Keycloak + Mongo + Kafka): recomendação de tratar GraalVM e Keycloak como trade-offs documentados | Decisão de não implementar GraalVM/Keycloak/Grafana e documentá-los |
| Padrões de mensageria | Correção conceitual: **Outbox** protege a *publicação*; **Inbox / Idempotent Consumer** protege o *consumo* | Adoção dos dois padrões |
| Estilo arquitetural | Sugestão de monólito modular + hexagonal | Adoção e organização dos pacotes **[confirmar]** |
| Resiliência | Mapeamento de mecanismos nativos do Spring equivalentes ao Resilience4j (retry, DLQ, idempotência, timeouts) | Decisão de não usar Resilience4j |
| Modelagem DDD | Discussão de agregado, value objects e eventos de domínio | Modelagem de `Invoice`, `ReconciliationRecord`, VOs e eventos selados **[confirmar]** |

## 2. Infraestrutura local

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Docker Compose do MongoDB | Diagnóstico de falhas de inicialização: `MONGO_INITDB_ROOT_*` ativa `--auth`, e replica set + auth exige `--keyFile` | Remoção da autenticação local, documentada como trade-off; serviço `mongo-init` idempotente |
| Redpanda Console | Correção da imagem do console | Configuração do compose **[confirmar]** |

## 3. Implementação e revisão de código

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Domínio, serviço de aplicação, adapters Mongo/Kafka, outbox | Revisões de código e explicações | **Implementação minha** |
| Revisão pós-outbox (22/09) | A revisão apontou: (1) `DuplicateKeyException` capturada **dentro** da transação — no Mongo o erro aborta a transação, então duplicatas iriam para a DLQ; (2) corrida entre dois pagamentos da mesma fatura; (3) pagamento com valor divergente podia **reverter** fatura paga para `ABERTA`; (4) `expectedAmount` perdido nos eventos | **Correções implementadas por mim**: verificação prévia de idempotência, update condicional (compare-and-set), `updatedInvoice` só em transição real; iterações revisadas pela IA até ficarem corretas |
| Testes unitários adicionais | **Gerados pela IA a meu pedido** (motor, `Invoice`, serviço de conciliação; depois relay do outbox, compare-and-set, value objects, mapeamentos) | Revisão e execução |
| API REST + OpenAPI | Planejamento dos endpoints com a IA; **implementação feita pela IA a meu pedido** (prazo) | Escopo e prioridades dos endpoints; revisão; verificação manual no ambiente |
| Bugs encontrados durante a API | IA identificou `toDomain` quebrando em registros sem `txId`/`expectedAmount` e índice de fallback apontando para campo inexistente | Validação no ambiente |
| Preparação da carga | Desenho do gerador de carga e do mix de cenários; limite de lote no outbox | Aprovação do desenho e do mix |

## 4. Reta final (23/09) — implementação pela IA a meu pedido

Com o prazo antecipado, pedi que a IA implementasse o restante do plano. Cada bloco foi planejado, aprovado por mim,
executado com minha autorização comando a comando e commitado separadamente.

| Bloco | O que a IA fez | Minha participação |
|---|---|---|
| Métricas (Micrometer) | Porta `ReconciliationMetricsPort` + adapter Micrometer (contador por status/motivo, timer de latência com SLO de 2 s, contador de duplicatas), registro após o commit; registry Prometheus | Aprovação do desenho (porta na aplicação para manter o serviço livre de framework); revisão |
| Gerador de carga | `LoadScenarioPlan`, `InvoiceSeeder`, `PixLoadGenerator`, `PixLoadDemo` (burst e ritmo constante, esperado × obtido, DLT, percentis a partir do histograma) | Escolha de incluir o modo de ritmo constante; aprovação da rodada 12 × 12; leitura dos resultados |
| Diagnóstico de desempenho | Mostrou que a latência do burst era fila, que 12 × 12 não escalava e que o gargalo é o MongoDB (não o consumer); paralelismo tornou-se configurável | Decisão de limitar o diagnóstico ao time-box e documentar as evoluções |
| Testes de arquitetura | `ApplicationModules.verify()` + regras hexagonais com ArchUnit | Revisão |
| Testes de integração | Testcontainers (Mongo replica set + Redpanda): fluxo completo, idempotência, DLT, rollback real do compare-and-set | Revisão |
| Documentação | Preenchimento do README (diagramas Mermaid, resultados, trade-offs), roteiro da demo, este documento | Revisão final do texto **[confirmar]** |

**Achados relevantes da IA nessa etapa** (todos verificados pela execução):
- Testcontainers 2.x não sobe o MongoDB como replica set por padrão (`withReplicaSet()`), sem o que as transações
  falham.
- As fábricas Kafka customizadas são montadas a partir de `KafkaProperties` e ignoram os `ConnectionDetails` do Spring
  Boot — no primeiro teste de integração, o tráfego Kafka foi para o broker do docker-compose. Corrigido no teste e
  registrado como limitação.
- O `DefaultErrorHandler` do spring-kafka loga o payload (com a chave Pix) ao esgotar as retentativas — registrado como
  ponto de LGPD.
- A inconsistência de nomes `amount` × `transactionAmount` nos eventos já tinha sido corrigida no código; a pendência
  foi removida da documentação.

## 5. Documentação

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| README | Esqueleto e, na reta final, texto completo gerado pela IA a partir das decisões e dos resultados medidos | Revisão **[confirmar]** |
| Este documento | Gerado pela IA a partir do histórico | Revisão **[confirmar]** |

---

## O que a IA **não** fez / limites do uso

- A IA não decidiu escopo nem trade-offs sozinha: as decisões registradas no plano do projeto (sem Resilience4j,
  sem GraalVM/Keycloak/Grafana, Mongo local sem autenticação, corte do MDC) foram minhas, com a IA apresentando
  alternativas.
- Nenhum comando foi executado sem autorização, e nenhum commit foi feito sem meu aval.
- Toda sugestão foi validada: testes executados localmente, testes de integração com Testcontainers e verificação
  manual no ambiente Docker.
- **[confirmar]** decisões tomadas sem apoio da IA e casos em que uma sugestão da IA foi rejeitada ou corrigida por
  mim.
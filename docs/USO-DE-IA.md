# Uso de IA no desenvolvimento

> Documento preparado com apoio da própria IA a partir do histórico das sessões, revisado e confirmado por mim.

**Ferramenta**: Claude (Anthropic) — conversas de arquitetura e revisão ao longo do projeto e, na reta final, Claude
Code (CLI) com acesso ao repositório. Nenhuma outra ferramenta de IA foi usada.

**Modo de trabalho combinado**: na maior parte do projeto a IA atuou como **tutora/revisora** — explicações,
referências e revisão de código, com a implementação feita por mim. Nos dois últimos dias, por causa do prazo, pedi
explicitamente que a IA **implementasse** partes do projeto; essas partes estão listadas abaixo como "gerado pela IA
a meu pedido". Em todos os casos, o plano de cada etapa foi aprovado por mim antes da implementação, cada comando
(build, testes, Docker, commits) foi executado só com minha autorização, e o resultado foi revisado por mim.

---

## 1. Concepção e arquitetura (sessões iniciais)

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Leitura do case | Discussão dos NFRs e do que priorizar num prazo de 7 dias | Priorização final do escopo |
| Stack inicial | Crítica da proposta (Spring Boot + GraalVM + Keycloak + Mongo + Kafka): recomendação de tratar GraalVM e Keycloak como trade-offs documentados | Decisão de não implementar GraalVM/Keycloak/Grafana e documentá-los |
| Padrões de mensageria | Correção conceitual: **Outbox** protege a *publicação*; **Inbox / Idempotent Consumer** protege o *consumo* | Adoção dos dois padrões |
| Estilo arquitetural | Sugestão de monólito modular + hexagonal | Adoção e organização dos pacotes |
| Resiliência | Mapeamento de mecanismos nativos do Spring equivalentes ao Resilience4j (retry, DLQ, idempotência, timeouts) | Decisão de não usar Resilience4j |
| Modelagem DDD | Discussão de agregado, value objects e eventos de domínio | Modelagem de `Invoice`, `ReconciliationRecord`, VOs e eventos selados |

## 2. Infraestrutura local

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| Docker Compose do MongoDB | Diagnóstico de falhas de inicialização: `MONGO_INITDB_ROOT_*` ativa `--auth`, e replica set + auth exige `--keyFile` | Remoção da autenticação local, documentada como trade-off; serviço `mongo-init` idempotente |
| Redpanda Console | Correção da imagem do console | Configuração do compose |

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
| Documentação | Preenchimento do README (diagramas Mermaid, resultados, trade-offs), roteiro da demo, este documento | Revisão final do texto |

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

### Tarde de 23/09 — ajustes pós-ensaio e investigação do MongoDB

| Momento | Uso da IA | Minha participação / decisão |
|---|---|---|
| Mensagens de erro 400 | Pedi que o 400 de validação listasse os campos inválidos. **Implementado pela IA**: propriedade `errors` (campo + mensagem) no ProblemDetail, para corpo e query params, sem devolver o valor rejeitado (pode conter a chave Pix) | Pedido da mudança; aprovação da decisão de não ecoar o valor rejeitado |
| Valores exibidos como `150.0` | Pedi a correção. A IA verificou as respostas brutas antes de alterar código: a API já devolvia `150.00`; o `150.0` vinha do `Invoke-RestMethod` do PowerShell, usado no roteiro da demo. Correção no roteiro (helper que mostra o JSON bruto) + teste que fixa o formato. Ao testar o helper, achou outro problema: respostas `application/problem+json` chegam como bytes no PowerShell | Pedido; aprovação da correção no roteiro em vez de mudança na API |
| Desempenho do MongoDB | Perguntei como melhorar o banco, apontado como gargalo. A IA propôs caminhos priorizados por ganho × risco e recomendou medir antes de otimizar | **Escolhi** medir (#1) e testar a atualização em lote do relay do outbox (#2) |
| Medição | Detalhamento por comando a partir do timer `mongodb.driver.commands`: ~6,4 comandos e ~18 ms de Mongo por conciliação; `commitTransaction` (~6 ms, 32 %) é o maior custo isolado; operações simples a ~2 ms | Aprovação de cada execução |
| Atualização em lote do relay | Implementada e validada por teste de integração; a comparação antes × depois foi inconclusiva (todos os comandos ficaram mais lentos, inclusive os não afetados → ruído do ambiente) | Escolhi repetir com um experimento A/B (3 rodadas alternadas por variante) |
| Resultado final | O experimento A/B saiu **inválido por erro da IA** (ver abaixo). A IA identificou e reportou o erro pelos próprios números | **Decidi desfazer a mudança de código** e registrar no README o custo medido e os caminhos de otimização priorizados, sem prometer ganho não comprovado |

**Erros da IA nesta etapa** (identificados e corrigidos antes da entrega):
- No resumo do ensaio da demo, a IA apontou "valores aparecem como `150.0`" como problema da API sem verificar; a
  investigação posterior mostrou que era artefato do PowerShell.
- No experimento A/B, o script da IA copiava os fontes com `Copy-Item`, que preserva a data de modificação; o build
  incremental do Maven não recompilou a variante nova, e as duas versões testadas eram a original. A IA percebeu pelo
  número de escritas do relay (igual nas duas variantes), invalidou o experimento e fez build limpo (`mvn clean`)
  antes de continuar.
- O experimento serviu para uma conclusão: rodadas idênticas variaram de 215 a 287 conciliações/s neste notebook,
  então ganhos de 10–20 % não são mensuráveis de forma confiável nesse ambiente — registrado no README.

## 5. Documentação

| Momento | Uso da IA | Decisão / trabalho próprio |
|---|---|---|
| README | Esqueleto e, na reta final, texto completo gerado pela IA a partir das decisões e dos resultados medidos | Revisão |
| Este documento | Gerado pela IA a partir do histórico | Revisão e confirmação |

---

## O que a IA **não** fez / limites do uso

- A IA não decidiu escopo nem trade-offs sozinha: as decisões registradas no plano do projeto (sem Resilience4j,
  sem GraalVM/Keycloak/Grafana, Mongo local sem autenticação, corte do MDC) foram minhas, com a IA apresentando
  alternativas.
- Nenhum comando foi executado sem autorização, e nenhum commit foi feito sem meu aval.
- Toda sugestão foi validada: testes executados localmente, testes de integração com Testcontainers e verificação
  manual no ambiente Docker.
- Mudança da IA que descartei: a atualização em lote do relay do outbox foi implementada e testada, mas desfeita por
  decisão minha, porque o ganho não pôde ser comprovado e a entrega era no dia seguinte; ficou como evolução
  documentada.
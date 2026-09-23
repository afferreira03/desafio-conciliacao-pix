# Roteiro da demonstração (30 min) + pontos para a discussão (30 min)

Comandos em **PowerShell** (Windows). Todos os `endToEndId` têm 32 caracteres (`E` + 31 alfanuméricos), como exige o
value object `EndToEndId`.

---

## 0. Antes de começar (fora do tempo da demo)

```powershell
docker compose down -v            # banco e tópicos limpos (apaga os dados de dev!)
docker compose up -d
docker compose ps                 # mongodb/redpanda healthy, mongo-init "Exited (0)"
mvn spring-boot:run               # em outro terminal; aguardar "Started ConciliacaoPixApplication"
```

Abas abertas: Swagger UI (`http://localhost:8081/swagger-ui.html`), Redpanda Console (`http://localhost:8080`, aba
*Topics*), este roteiro, o README na seção 8.

Função auxiliar para publicar um Pix (cole no terminal da demo):

```powershell
function Send-Pix($key, $json) { "$key $json" | docker exec -i redpanda rpk topic produce pix.transactions -f '%k %v\n' }
$api = 'http://localhost:8081/api/v1'
```

---

## 1. Contexto e arquitetura — 4 min

- Problema: conciliar cada Pix recebido com a fatura correspondente, em ≤ 2 s, com 2k–7k TPS.
- README seção 2: monólito modular + hexagonal; diagrama de componentes e de sequência.
- Pontos-chave para citar já aqui: **idempotência**, **outbox transacional**, **compare-and-set**, **DLT**.

## 2. Abrir faturas — 3 min

Pelo Swagger (`POST /api/v1/invoices`) ou:

```powershell
$exp = (Get-Date).ToUniversalTime().AddDays(1).ToString('yyyy-MM-ddTHH:mm:ssZ')
Invoke-RestMethod -Method Post "$api/invoices" -ContentType 'application/json' -Body "{`"txId`":`"DEMO1`",`"amount`":150.00,`"pixKey`":`"loja@demo.com`",`"expiresAt`":`"$exp`"}"
Invoke-RestMethod -Method Post "$api/invoices" -ContentType 'application/json' -Body "{`"txId`":`"DEMO2`",`"amount`":80.00,`"pixKey`":`"loja@demo.com`",`"expiresAt`":`"$exp`"}"
Invoke-RestMethod -Method Post "$api/invoices" -ContentType 'application/json' -Body "{`"txId`":`"DEMO3`",`"amount`":42.50,`"pixKey`":`"cliente.fallback@demo.com`",`"expiresAt`":`"$exp`"}"
Invoke-RestMethod "$api/invoices/DEMO1"      # status ABERTA, chave Pix mascarada (LGPD)
```

Mostrar também um **409** (repetir o POST de `DEMO1`) e um **400** (valor negativo) → ProblemDetail.

## 3. Os três status ao vivo — 8 min

```powershell
$now = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')

# CONCILIADO
Send-Pix 'DEMO1' "{`"endToEndId`":`"E0000000020260924100000000000001`",`"txId`":`"DEMO1`",`"transactionAmount`":150.00,`"paymentTimestamp`":`"$now`",`"pixKey`":`"loja@demo.com`"}"
Invoke-RestMethod "$api/reconciliations/E0000000020260924100000000000001"
Invoke-RestMethod "$api/invoices/DEMO1"      # agora PAGA

# INCONSISTENTE — valor divergente
Send-Pix 'DEMO2' "{`"endToEndId`":`"E0000000020260924100000000000002`",`"txId`":`"DEMO2`",`"transactionAmount`":79.90,`"paymentTimestamp`":`"$now`",`"pixKey`":`"loja@demo.com`"}"
Invoke-RestMethod "$api/reconciliations/E0000000020260924100000000000002"   # AMOUNT_MISMATCH, expected 80.00

# INCONSISTENTE — segundo pagamento da fatura já paga
Send-Pix 'DEMO1' "{`"endToEndId`":`"E0000000020260924100000000000003`",`"txId`":`"DEMO1`",`"transactionAmount`":150.00,`"paymentTimestamp`":`"$now`",`"pixKey`":`"loja@demo.com`"}"
Invoke-RestMethod "$api/reconciliations/E0000000020260924100000000000003"   # INVOICE_ALREADY_PAID

# PENDENTE — fatura inexistente
Send-Pix 'NAOEXISTE' "{`"endToEndId`":`"E0000000020260924100000000000004`",`"txId`":`"NAOEXISTE`",`"transactionAmount`":10.00,`"paymentTimestamp`":`"$now`",`"pixKey`":`"loja@demo.com`"}"
Invoke-RestMethod "$api/reconciliations/E0000000020260924100000000000004"

# CONCILIADO por fallback — sem txId: chave Pix + valor + janela de ±30 min
Send-Pix 'E0000000020260924100000000000005' "{`"endToEndId`":`"E0000000020260924100000000000005`",`"txId`":null,`"transactionAmount`":42.50,`"paymentTimestamp`":`"$now`",`"pixKey`":`"cliente.fallback@demo.com`"}"
Invoke-RestMethod "$api/invoices/DEMO3"      # PAGA
```

No Redpanda Console: tópico `pix.reconciliation.result` com um evento por conciliação (publicado pelo **outbox**).

## 4. Idempotência e DLT — 4 min

```powershell
# Reentrega exata do primeiro Pix → nenhum registro novo
Send-Pix 'DEMO1' "{`"endToEndId`":`"E0000000020260924100000000000001`",`"txId`":`"DEMO1`",`"transactionAmount`":150.00,`"paymentTimestamp`":`"$now`",`"pixKey`":`"loja@demo.com`"}"
Invoke-RestMethod "$api/reconciliations?size=10"                                   # continua 5 registros
Invoke-RestMethod 'http://localhost:8081/actuator/metrics/pix.reconciliation.duplicates'

# Mensagem envenenada → DLT (sem retry: erro de desserialização)
Send-Pix 'LIXO' '{isto nao e json'
```

No Console: `pix.transactions.DLT` com a mensagem original. Explicar: erro transitório → 3 retentativas com backoff;
erro de desserialização → direto para o DLT.

## 5. Relatório — 2 min

```powershell
Invoke-RestMethod "$api/reconciliations/summary" | ConvertTo-Json -Depth 5
Invoke-RestMethod "$api/reconciliations?status=INCONSISTENTE&reason=AMOUNT_MISMATCH"
```

## 6. Carga — 6 min

```powershell
mvn -q test-compile exec:java "-Dexec.classpathScope=test" "-Dexec.mainClass=br.com.desafio.conciliacaopix.loadtest.PixLoadDemo" "-Dexec.args=5000" "-Dloadtest.rate=200"
```

~25 s de envio. Enquanto roda: Console → consumer group `pix-reconciliation-group` (lag perto de zero). Ao final:
tabela **esperado × obtido** toda OK, 0 no DLT, p99 < 1 s, 100 % dentro do SLO de 2 s.

Em seguida, README seção 8: o **burst** mostra a capacidade de ~300/s neste notebook; 12 × 12 não melhora → o gargalo
é o MongoDB → argumento de escala (sharding, bulk writes, CDC).

Mostrar também: `Invoke-RestMethod "$api/reconciliations/summary"` (os números do lote aparecem somados).

## 7. Testes — 3 min

```powershell
mvn test        # ~1 min: 102 testes unitários e de arquitetura, sem Docker
```

`mvn verify` (integração com Testcontainers) leva ~2–3 min: rodar antes e mostrar o resultado, ou deixar rodando
durante a discussão. Destacar `ReconciliationFlowIT.compareAndSetRollsBackWholeTransaction` (rollback real no Mongo).

---

## Pontos para a discussão (30 min)

**Decisões e por quês**
- Monólito modular + hexagonal: domínio sem framework (verificado por ArchUnit); pronto para extrair módulos.
- Outbox × dual-write; idempotência antes da transação (erro de escrita aborta transação Mongo — README seção 7).
- Compare-and-set em vez de lock: sem estado de lock, conflito vira rollback + retry + `INVOICE_ALREADY_PAID`.
- Chave de partição `txId`: ordem por fatura; hot key desprezível.
- Por que não Resilience4j / Keycloak / GraalVM / Grafana — README seção 12.

**Números**
- 2 s atendido abaixo da capacidade; burst = fila. Capacidade medida ~300/s em um nó Docker Desktop.
- Dobrar consumers não escalou → gargalo no banco → evolução concreta, não genérica.

**Limitações honestas** (README seção 13)
- `PENDENTE` não é reprocessado quando a fatura chega depois (conflita com a idempotência — precisa de um fluxo
  próprio de reconciliação tardia).
- Sem replay do DLT, sem expurgo do outbox, contrato de evento acoplado ao domínio.
- Fábricas Kafka customizadas ignoram `ConnectionDetails` (descoberto nos testes de integração).
- LGPD: payload no log do `DefaultErrorHandler`; criptografia de campo como evolução.

**Uso de IA** — `docs/USO-DE-IA.md`: onde ajudou, o que foi gerado a pedido, o que foi decidido e revisado por mim.
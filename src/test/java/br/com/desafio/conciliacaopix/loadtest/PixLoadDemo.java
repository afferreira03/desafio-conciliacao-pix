package br.com.desafio.conciliacaopix.loadtest;

import br.com.desafio.conciliacaopix.loadtest.LoadScenarioPlan.Expected;
import br.com.desafio.conciliacaopix.loadtest.LoadScenarioPlan.InvoiceSeed;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Demonstração de carga (burst) — evidência para os NFRs de vazão e latência e para a corretude sob carga.
 * <p>
 * Pré-requisitos: {@code docker compose up -d} e a aplicação rodando. Execução pela IDE (método {@code main}) ou:
 * <pre>
 * mvn -q test-compile exec:java -Dexec.classpathScope=test \
 *     -Dexec.mainClass=br.com.desafio.conciliacaopix.loadtest.PixLoadDemo -Dexec.args="5000"
 * </pre>
 * Propriedades opcionais: {@code -Dloadtest.baseUrl} (padrão {@code http://localhost:8081}),
 * {@code -Dloadtest.bootstrap} (padrão {@code localhost:19092}), {@code -Dloadtest.rate} (msg/s; padrão 0 = burst).
 * <p>
 * A latência vem do histograma do timer {@code pix.reconciliation.latency} da aplicação (antes × depois da rodada).
 */
public final class PixLoadDemo {

    private static final String PIX_TOPIC = "pix.transactions";
    private static final String DLT_TOPIC = "pix.transactions.DLT";
    private static final Duration COMPLETION_TIMEOUT = Duration.ofMinutes(5);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String LATENCY_BUCKET = "pix_reconciliation_latency_seconds_bucket";
    private static final Pattern LE = Pattern.compile("le=\"([^\"]+)\"");

    private final String baseUrl;
    private final String bootstrap;
    private final int rate = Integer.getInteger("loadtest.rate", 0);
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final List<String[]> checks = new ArrayList<>();

    private PixLoadDemo(String baseUrl, String bootstrap) {
        this.baseUrl = baseUrl;
        this.bootstrap = bootstrap;
    }

    public static void main(String[] args) throws Exception {
        int total = args.length > 0 ? Integer.parseInt(args[0]) : 5_000;
        PixLoadDemo demo = new PixLoadDemo(
                System.getProperty("loadtest.baseUrl", "http://localhost:8081"),
                System.getProperty("loadtest.bootstrap", "localhost:19092"));
        boolean ok = demo.run(total);
        System.exit(ok ? 0 : 1);
    }

    private boolean run(int total) throws Exception {
        Instant runStart = Instant.now();
        LoadScenarioPlan plan = LoadScenarioPlan.create(total);
        Expected expected = plan.expected();

        System.out.printf("%n=== Demo de carga Pix — run %s — %s ===%n", plan.runId(), runStart);
        System.out.println("Mix de cenários:");
        plan.counts().forEach((scenario, count) -> System.out.printf("  %-16s %6d%n", scenario, count));
        System.out.printf("Esperado: %d mensagens → %d conciliações | CONCILIADO %d | INCONSISTENTE %d "
                        + "(AMOUNT_MISMATCH %d, INVOICE_ALREADY_PAID %d) | PENDENTE %d | faturas PAGA %d%n%n",
                expected.messages(), expected.records(), expected.conciliado(), expected.inconsistente(),
                expected.amountMismatch(), expected.alreadyPaid(), expected.pendente(), expected.invoicesPaid());

        long dltBefore = dltEndOffsets();
        NavigableMap<Double, Double> latencyBefore = latencyHistogram();

        System.out.printf("[1/4] Semeando %d faturas via API...%n", plan.invoices().size());
        new InvoiceSeeder(http, baseUrl).seed(plan.invoices());

        System.out.printf("[2/4] Publicando %d mensagens em '%s' (%s)...%n", expected.messages(), PIX_TOPIC,
                rate > 0 ? "ritmo constante de " + rate + " msg/s" : "burst");
        Instant produceStart = Instant.now();
        PixLoadGenerator.Result produced = new PixLoadGenerator(bootstrap, PIX_TOPIC, rate)
                .publish(plan.phaseOne(), plan.phaseTwo());
        System.out.printf("Produção: %d enviadas, %d falhas em %d ms → %.0f msg/s%n",
                produced.sent(), produced.failed(), produced.elapsed().toMillis(), produced.rate());

        System.out.println("[3/4] Aguardando a conciliação de todas as mensagens...");
        JsonNode summary = awaitCompletion(runStart, expected.records());
        Duration endToEnd = Duration.between(produceStart, Instant.now());
        long totalCount = summary.path("totalCount").asLong();
        System.out.printf("Fim a fim: %d conciliações em %d ms → %.0f conciliações/s%n",
                totalCount, endToEnd.toMillis(), totalCount / InvoiceSeeder.seconds(endToEnd));

        // Janela extra para detectar registros a mais (ex.: reenvio gerando conciliação nova).
        Thread.sleep(2_000);
        summary = summary(runStart);

        System.out.println("[4/4] Verificando resultados...");
        Map<String, Long> byStatus = countsBy(summary.path("statuses"), "status");
        Map<String, Long> byReason = new HashMap<>();
        for (JsonNode status : summary.path("statuses")) {
            byReason.putAll(countsBy(status.path("reasons"), "reason"));
        }
        long invoicesPaid = countInvoices(plan.invoices(), true);
        long invoicesStillOpen = countInvoices(plan.invoices(), false);
        long dltDelta = dltEndOffsets() - dltBefore;

        check("Mensagens publicadas", expected.messages(), produced.sent());
        check("Conciliações (mensagens − reenvios)", expected.records(), summary.path("totalCount").asLong());
        check("CONCILIADO", expected.conciliado(), byStatus.getOrDefault("CONCILIADO", 0L));
        check("INCONSISTENTE", expected.inconsistente(), byStatus.getOrDefault("INCONSISTENTE", 0L));
        check("  AMOUNT_MISMATCH", expected.amountMismatch(), byReason.getOrDefault("AMOUNT_MISMATCH", 0L));
        check("  INVOICE_ALREADY_PAID", expected.alreadyPaid(), byReason.getOrDefault("INVOICE_ALREADY_PAID", 0L));
        check("PENDENTE", expected.pendente(), byStatus.getOrDefault("PENDENTE", 0L));
        check("Faturas PAGA", expected.invoicesPaid(), invoicesPaid);
        check("Faturas divergentes ainda ABERTA", expected.amountMismatch(), invoicesStillOpen);
        check("Mensagens no DLT", 0, dltDelta);

        System.out.printf("%n%-38s %10s %10s  %s%n", "Verificação", "Esperado", "Obtido", "");
        checks.forEach(c -> System.out.printf("%-38s %10s %10s  %s%n", c[0], c[1], c[2], c[3]));

        printLatency(latencyBefore);

        boolean allOk = checks.stream().allMatch(c -> c[3].equals("OK"));
        System.out.println(allOk ? "\nRESULTADO: OK — esperado = obtido em todas as verificações."
                : "\nRESULTADO: FALHA — ver linhas marcadas acima.");
        return allOk;
    }

    private JsonNode awaitCompletion(Instant from, long expectedRecords) throws Exception {
        Instant deadline = Instant.now().plus(COMPLETION_TIMEOUT);
        long lastCount = -1;
        while (Instant.now().isBefore(deadline)) {
            JsonNode summary = summary(from);
            long count = summary.path("totalCount").asLong();
            if (count >= expectedRecords) {
                return summary;
            }
            if (count != lastCount && count / 500 != lastCount / 500) {
                System.out.printf("  ... %d / %d%n", count, expectedRecords);
            }
            lastCount = count;
            Thread.sleep(250);
        }
        System.err.printf("  timeout de %s aguardando %d conciliações (obtidas: %d)%n",
                COMPLETION_TIMEOUT, expectedRecords, lastCount);
        return summary(from);
    }

    private JsonNode summary(Instant from) throws Exception {
        return getJson("/api/v1/reconciliations/summary?from=" + from);
    }

    private long countInvoices(List<InvoiceSeed> invoices, boolean expectPaid) throws InterruptedException {
        String wantedStatus = expectPaid ? "PAGA" : "ABERTA";
        List<InvoiceSeed> subset = invoices.stream().filter(i -> i.expectPaid() == expectPaid).toList();
        AtomicLong matching = new AtomicLong();
        Semaphore inFlight = new Semaphore(64);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (InvoiceSeed invoice : subset) {
                inFlight.acquire();
                executor.submit(() -> {
                    try {
                        if (wantedStatus.equals(getJson("/api/v1/invoices/" + invoice.txId()).path("status").asString())) {
                            matching.incrementAndGet();
                        }
                    } catch (Exception e) {
                        System.err.println("  falha ao consultar fatura " + invoice.txId() + ": " + e);
                    } finally {
                        inFlight.release();
                    }
                });
            }
        }
        return matching.get();
    }

    /**
     * Percentis calculados a partir do histograma exportado em {@code /actuator/prometheus} — mesma técnica do
     * {@code histogram_quantile} do Prometheus. Usa a diferença entre os buckets antes/depois da execução, então o
     * resultado é só desta rodada (sem precisar reiniciar a aplicação).
     */
    private void printLatency(NavigableMap<Double, Double> before) {
        try {
            NavigableMap<Double, Double> after = latencyHistogram();
            NavigableMap<Double, Double> delta = new TreeMap<>();
            after.forEach((le, count) -> delta.put(le, count - before.getOrDefault(le, 0.0)));
            double total = delta.isEmpty() ? 0 : delta.lastEntry().getValue();

            System.out.printf("%nLatência desta rodada (paymentTimestamp → conciliação persistida), %.0f amostras:%n", total);
            if (total == 0) {
                System.out.println("  sem amostras");
                return;
            }
            for (double q : new double[]{0.50, 0.95, 0.99}) {
                System.out.printf("  p%-3d %8.0f ms%n", Math.round(q * 100), quantile(delta, q * total) * 1000);
            }
            double withinSlo = delta.getOrDefault(2.0, 0.0);
            System.out.printf("  dentro do SLO de 2 s: %.0f de %.0f (%.2f%%)%n", withinSlo, total, withinSlo * 100 / total);
        } catch (Exception e) {
            System.err.println("  não foi possível ler as métricas de latência: " + e);
        }
    }

    /** Buckets cumulativos {@code le → contagem} do timer de latência. */
    private NavigableMap<Double, Double> latencyHistogram() throws Exception {
        NavigableMap<Double, Double> buckets = new TreeMap<>();
        getText("/actuator/prometheus").lines()
                .filter(l -> l.startsWith(LATENCY_BUCKET + "{"))
                .forEach(l -> {
                    Matcher m = LE.matcher(l);
                    if (m.find()) {
                        double le = m.group(1).equals("+Inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(m.group(1));
                        buckets.merge(le, Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1)), Double::sum);
                    }
                });
        return buckets;
    }

    /** Interpolação linear dentro do bucket que contém a posição {@code rank}. */
    private static double quantile(NavigableMap<Double, Double> cumulative, double rank) {
        double previousLe = 0;
        double previousCount = 0;
        for (Map.Entry<Double, Double> bucket : cumulative.entrySet()) {
            if (bucket.getValue() >= rank) {
                if (bucket.getKey().isInfinite()) {
                    return previousLe; // acima do maior bucket finito: melhor estimativa disponível
                }
                double inBucket = bucket.getValue() - previousCount;
                double fraction = inBucket == 0 ? 1 : (rank - previousCount) / inBucket;
                return previousLe + (bucket.getKey() - previousLe) * fraction;
            }
            previousLe = bucket.getKey();
            previousCount = bucket.getValue();
        }
        return previousLe;
    }

    private long dltEndOffsets() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(props)) {
            TopicDescription description = admin.describeTopics(List.of(DLT_TOPIC)).allTopicNames().get().get(DLT_TOPIC);
            Map<TopicPartition, OffsetSpec> request = description.partitions().stream()
                    .map(p -> new TopicPartition(DLT_TOPIC, p.partition()))
                    .collect(Collectors.toMap(Function.identity(), _ -> OffsetSpec.latest()));
            return admin.listOffsets(request).all().get().values().stream().mapToLong(o -> o.offset()).sum();
        } catch (Exception e) {
            System.err.println("  não foi possível ler offsets do DLT (" + e.getMessage() + ") — considerando 0");
            return 0;
        }
    }

    private void check(String label, long expected, long actual) {
        checks.add(new String[]{label, String.valueOf(expected), String.valueOf(actual), expected == actual ? "OK" : "FALHA"});
    }

    private JsonNode getJson(String path) throws Exception {
        return JSON.readTree(getText(path));
    }

    private String getText(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " → HTTP " + response.statusCode() + " " + response.body());
        }
        return response.body();
    }

    private static Map<String, Long> countsBy(JsonNode array, String field) {
        Map<String, Long> result = new HashMap<>();
        for (JsonNode node : array) {
            result.merge(node.path(field).asString(), node.path("count").asLong(), Long::sum);
        }
        return result;
    }
}
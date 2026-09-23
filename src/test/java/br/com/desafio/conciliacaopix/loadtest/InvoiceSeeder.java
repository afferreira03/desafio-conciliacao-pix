package br.com.desafio.conciliacaopix.loadtest;

import br.com.desafio.conciliacaopix.loadtest.LoadScenarioPlan.InvoiceSeed;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cria as faturas pela própria API ({@code POST /api/v1/invoices}) — garante o mesmo esquema que a aplicação grava
 * (Decimal128, enums, índices) e exercita a API. Threads virtuais com concorrência limitada para não saturar o Tomcat.
 */
public final class InvoiceSeeder {

    private static final int MAX_IN_FLIGHT = 64;

    private final HttpClient httpClient;
    private final String baseUrl;

    public InvoiceSeeder(HttpClient httpClient, String baseUrl) {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl;
    }

    public Duration seed(List<InvoiceSeed> invoices) throws InterruptedException {
        Instant expiresAt = Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
        Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
        AtomicInteger created = new AtomicInteger();
        ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();

        long start = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (InvoiceSeed invoice : invoices) {
                inFlight.acquire();
                executor.submit(() -> {
                    try {
                        HttpResponse<String> response = httpClient.send(request(invoice, expiresAt),
                                HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() == 201) {
                            created.incrementAndGet();
                        } else {
                            failures.add(invoice.txId() + " → HTTP " + response.statusCode() + " " + response.body());
                        }
                    } catch (Exception e) {
                        failures.add(invoice.txId() + " → " + e);
                    } finally {
                        inFlight.release();
                    }
                });
            }
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        if (!failures.isEmpty()) {
            failures.stream().limit(5).forEach(f -> System.err.println("  falha ao criar fatura: " + f));
            throw new IllegalStateException(failures.size() + " faturas não foram criadas (de " + invoices.size() + ")");
        }
        System.out.printf("Faturas criadas: %d em %d ms (%.0f req/s)%n",
                created.get(), elapsed.toMillis(), created.get() / seconds(elapsed));
        return elapsed;
    }

    private HttpRequest request(InvoiceSeed invoice, Instant expiresAt) {
        String body = """
                {"txId":"%s","amount":%s,"pixKey":"%s","expiresAt":"%s"}"""
                .formatted(invoice.txId(), invoice.amount().toPlainString(), invoice.pixKey(), expiresAt);
        return HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/invoices"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    static double seconds(Duration duration) {
        return Math.max(duration.toNanos(), 1) / 1_000_000_000.0;
    }
}
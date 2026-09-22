package br.com.desafio.conciliacaopix.reconciliation.infrastructure.config;


import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.timeknobs.outbox")
public record SchedulerTimeKnobs(
        long sendTimeoutMs,
        long pollIntervalMs
) {
}

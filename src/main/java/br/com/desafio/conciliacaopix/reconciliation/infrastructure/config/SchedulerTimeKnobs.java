package br.com.desafio.conciliacaopix.reconciliation.infrastructure.config;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "app.timeknobs.outbox")
public record SchedulerTimeKnobs(
        long sendTimeoutMs,
        long pollIntervalMs,
        @DefaultValue("500") int batchSize
) {
}

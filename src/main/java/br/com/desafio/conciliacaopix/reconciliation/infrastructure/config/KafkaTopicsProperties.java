package br.com.desafio.conciliacaopix.reconciliation.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka.topics")
public record KafkaTopicsProperties(
        String pixTransactions,
        String pixTransactionsDlt,
        String pixReconciliationResult
) {
}

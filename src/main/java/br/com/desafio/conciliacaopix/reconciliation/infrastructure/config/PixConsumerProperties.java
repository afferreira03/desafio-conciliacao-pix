package br.com.desafio.conciliacaopix.reconciliation.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Paralelismo do consumo de {@code pix.transactions}: partições do tópico (e do DLT) e threads do listener.
 * Threads acima do número de partições ficam ociosas; a escala horizontal (mais instâncias) também é limitada
 * pelo número de partições.
 */
@ConfigurationProperties(prefix = "app.kafka.consumer")
public record PixConsumerProperties(
        @DefaultValue("6") int partitions,
        @DefaultValue("6") int concurrency
) {
}
package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.messaging.kafka.config;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.KafkaTopicsProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Configuration
public class OutboxKafkaProducerConfig {

    private final KafkaTopicsProperties kafkaTopics;

    public OutboxKafkaProducerConfig(KafkaTopicsProperties kafkaTopics) {
        this.kafkaTopics = kafkaTopics;
    }

    @Bean
    public NewTopic pixReconciliationResult() {
        return TopicBuilder
                .name(kafkaTopics.pixReconciliationResult())
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}

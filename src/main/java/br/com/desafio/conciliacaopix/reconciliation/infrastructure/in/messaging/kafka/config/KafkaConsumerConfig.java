package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka.config;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.KafkaTopicsProperties;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka.dto.PixTransactionEventDto;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private final KafkaTopicsProperties kafkaTopics;

    public KafkaConsumerConfig(KafkaTopicsProperties kafkaTopics) {
        this.kafkaTopics = kafkaTopics;
    }

    @Bean
    public NewTopic pixTransactionTopic() {
        return topic(kafkaTopics.pixTransactions(), 6);
    }

    @Bean
    public NewTopic pixTransactionsDLTTopic() {
        return topic(kafkaTopics.pixTransactionsDlt(), 6);
    }

    @Bean
    public ConsumerFactory<String, PixTransactionEventDto> consumerFactory(KafkaProperties kafkaProperties) {
        JacksonJsonDeserializer<PixTransactionEventDto> jsonDeserializer =
                new JacksonJsonDeserializer<>(PixTransactionEventDto.class)
                        .trustedPackages("*")
                        .ignoreTypeHeaders();

        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    @Bean
    public KafkaOperations<String, byte[]> dltBytesTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public KafkaOperations<String, Object> dltJsonTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        props.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PixTransactionEventDto> kafkaListenerContainerFactory(
            ConsumerFactory<String, PixTransactionEventDto> consumerFactory,
            @Qualifier("dltBytesTemplate") KafkaOperations<String, byte[]> dltBytesTemplate,
            @Qualifier("dltJsonTemplate") KafkaOperations<String, Object> dltJsonTemplate) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, PixTransactionEventDto>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(6);

        Map<Class<?>, KafkaOperations<?, ?>> dltTemplates = new LinkedHashMap<>();
        dltTemplates.put(byte[].class, dltBytesTemplate);
        dltTemplates.put(Object.class, dltJsonTemplate);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dltTemplates,
                (consumerRecord, _) ->
                        new TopicPartition(kafkaTopics.pixTransactionsDlt(), consumerRecord.partition())
        );

        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L)));
        return factory;
    }

    private NewTopic topic(String name, int partitions) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(1)
                .build();
    }
}

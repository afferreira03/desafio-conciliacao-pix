package br.com.desafio.conciliacaopix.integration;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * Infra real para os testes de integração. As mesmas imagens do docker-compose:
 * <ul>
 *     <li>{@code mongo:7.0} em replica set de um nó ({@code withReplicaSet()}), então as transações
 *     multi-documento (registro + fatura + outbox) rodam de verdade;</li>
 *     <li>Redpanda (API Kafka).</li>
 * </ul>
 * O Kafka é ligado por {@code spring.kafka.bootstrap-servers} e não por {@code @ServiceConnection}: as fábricas
 * de consumer/producer da aplicação são montadas a partir de {@code KafkaProperties}, que não enxerga os
 * {@code ConnectionDetails} do Spring Boot — com {@code @ServiceConnection} elas continuariam apontando para o
 * broker do {@code application.yaml}.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    // Testcontainers 2.x: o replica set é opt-in (sem ele o Mongo recusa transações).
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0").withReplicaSet();
    static final RedpandaContainer REDPANDA = new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:latest");

    static {
        REDPANDA.start(); // removido ao final pelo Ryuk do Testcontainers
    }

    @Bean
    @ServiceConnection
    MongoDBContainer mongoDBContainer() {
        return MONGO;
    }

    @Bean
    DynamicPropertyRegistrar kafkaProperties() {
        return registry -> registry.add("spring.kafka.bootstrap-servers", REDPANDA::getBootstrapServers);
    }
}
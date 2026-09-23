package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.messaging.kafka;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataOutboxRepository;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.KafkaTopicsProperties;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.SchedulerTimeKnobs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OutboxEventPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxEventPoller.class);

    private final SchedulerTimeKnobs outboxTimeKnobs;
    private final KafkaTopicsProperties kafkaTopics;
    private final SpringDataOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxEventPoller(SchedulerTimeKnobs outboxTimeKnobs,
                             KafkaTopicsProperties kafkaTopics,
                             SpringDataOutboxRepository repository,
                             @Qualifier("outboxKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxTimeKnobs = outboxTimeKnobs;
        this.kafkaTopics = kafkaTopics;
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${app.timeknobs.outbox.poll-interval-ms:5000}")
    public void poll() {
        List<OutboxEventDocument> pending = repository.findByStatus(OutboxEventStatus.PENDING);
        if (pending.isEmpty()) return;

        Map<OutboxEventDocument, CompletableFuture<SendResult<String, String>>> futureMap = new LinkedHashMap<>();

        for (OutboxEventDocument event : pending) {
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(
                    kafkaTopics.pixReconciliationResult(),
                    event.getEndToEndId(),
                    event.getPayload()
            );
            futureMap.put(event, future);
        }

        CompletableFuture<Void> allSends = CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[0]));

        try {
            allSends.get(outboxTimeKnobs.sendTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.warn("Outbox batch não foi completamente concluída dentro dos {} ms.", outboxTimeKnobs.sendTimeoutMs());
        } catch (Exception e) {
            LOGGER.warn("Outbox batch completado com pelo menos uma falha", e);
        }

        List<OutboxEventDocument> sent = new ArrayList<>();

        futureMap.forEach((event, future) -> {
            if (future.isDone() && !future.isCompletedExceptionally()) {
                event.setStatus(OutboxEventStatus.SENT);
                event.setSentAt(Instant.now());
                sent.add(event);
            } else {
                LOGGER.warn("Falha ao publicar evento de outbox {} - Será tentado novmente no próximo poll.", event.getId());
            }
        });

        if (!sent.isEmpty()) {
            repository.saveAll(sent);
        }
    }
}

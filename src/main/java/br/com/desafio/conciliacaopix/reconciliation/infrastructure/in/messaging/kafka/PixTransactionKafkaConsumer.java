package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconcilePixTransactionUseCase;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.KafkaTopicsProperties;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka.dto.PixTransactionEventDto;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class PixTransactionKafkaConsumer {

    private final ReconcilePixTransactionUseCase useCase;

    @KafkaListener(
            topics = "${app.kafka.topics.pix-transactions:pix.transactions}",
            groupId = "${spring.kafka.consumer.group-id:pix-reconciliation-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(PixTransactionEventDto eventDto){
        log.info("Mensagem Pix recebida do kafka: endToEndId={}, txId={}, ", eventDto.endToEndId(), eventDto.txId());
        useCase.reconcile(eventDto.toCommand());
    }
}

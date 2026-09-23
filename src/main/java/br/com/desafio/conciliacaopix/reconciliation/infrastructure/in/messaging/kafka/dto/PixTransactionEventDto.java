package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka.dto;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconciliationPixCommand;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record PixTransactionEventDto(
        @JsonProperty("endToEndId") String endToEndId,
        @JsonProperty("txId") String txId,
        @JsonProperty("transactionAmount") BigDecimal amount,
        @JsonProperty("paymentTimestamp") Instant paymentTimestamp,
        @JsonProperty("pixKey") String pixKey
) {
    public ReconciliationPixCommand toCommand() {
        return new ReconciliationPixCommand(
                endToEndId,
                txId,
                Money.of(amount),
                paymentTimestamp != null ? paymentTimestamp : Instant.now(),
                pixKey
        );
    }
}

package br.com.desafio.conciliacaopix.reconciliation.application.port.in;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.PixTransaction;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.time.Instant;
import java.util.Objects;

public record ReconciliationPixCommand(
        String endToEndId,
        String txId,
        Money amount,
        Instant paymentTimestamp,
        String pixKey
) {
    public ReconciliationPixCommand {
        Objects.requireNonNull(endToEndId, "EndToEndId é obrigatório.");
        Objects.requireNonNull(amount, "Amount é obrigatório.");
        Objects.requireNonNull(paymentTimestamp, "PaymentTimestamp é obrigatório.");
    }

    public PixTransaction toDomain() {
        return new PixTransaction(
                EndToEndId.of(endToEndId),
                txId != null && !txId.isBlank() ? TxId.of(txId) : null,
                amount,
                paymentTimestamp,
                pixKey
        );
    }
}

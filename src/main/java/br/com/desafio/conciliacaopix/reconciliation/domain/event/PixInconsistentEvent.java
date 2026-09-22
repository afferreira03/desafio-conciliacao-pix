package br.com.desafio.conciliacaopix.reconciliation.domain.event;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.time.Instant;
import java.util.Objects;

public record PixInconsistentEvent(
        String reconciliationId,
        EndToEndId endToEndId,
        TxId txId,
        Money transactionAmout,
        Money expectedAmount,
        InconsistencyReason reason,
        Instant occurredOn
) implements DomainEvent {

    public PixInconsistentEvent {
        Objects.requireNonNull(reconciliationId, "reconciliationID");
        Objects.requireNonNull(endToEndId, "endToEndId");
        Objects.requireNonNull(transactionAmout, "transactionAmout");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(occurredOn, "occurredOn");
    }

    public static PixInconsistentEvent of(String reconciliationId, EndToEndId endToEndId, TxId txId,
                                          Money transactionAmount, Money expectedAmount, InconsistencyReason reason) {
        return new PixInconsistentEvent(reconciliationId, endToEndId, txId, transactionAmount, expectedAmount, reason, Instant.now());
    }
}

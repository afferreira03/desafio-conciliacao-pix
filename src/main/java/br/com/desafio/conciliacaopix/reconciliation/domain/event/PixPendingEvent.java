package br.com.desafio.conciliacaopix.reconciliation.domain.event;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.time.Instant;
import java.util.Objects;

public record PixPendingEvent(
        String reconciliationId,
        EndToEndId endToEndId,
        TxId txId,
        Money amount,
        Instant occurredOn
) implements DomainEvent {

    public PixPendingEvent {
        Objects.requireNonNull(reconciliationId, "ReconciliationId não pode ser vazio/nulo.");
        Objects.requireNonNull(endToEndId, "EndToEndId não pode ser vazio/nulo.");
        Objects.requireNonNull(amount, "Amount não pode ser vazio/nulo.");
        Objects.requireNonNull(occurredOn, "OccurredOn não pode ser vazio/nulo.");
    }

    public static PixPendingEvent of(String reconciliationId, EndToEndId endToEndId, TxId txId, Money amount) {
        return new PixPendingEvent(reconciliationId, endToEndId, txId, amount, Instant.now());
    }
}

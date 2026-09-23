package br.com.desafio.conciliacaopix.reconciliation.domain.event;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.time.Instant;
import java.util.Objects;

public record PixReconciledEvent(
        String reconciliationId,
        EndToEndId endToEndId,
        TxId txId,
        Money transactionAmount,
        Instant occurredOn
) implements DomainEvent {

    public PixReconciledEvent {
        Objects.requireNonNull(reconciliationId, "ReconciliationId não pode ser vazio/nulo.");
        Objects.requireNonNull(endToEndId, "EndToEndId é obrigatório e não pode ser vazio/nulo.");
        Objects.requireNonNull(transactionAmount, "Amount não pode ser vazio/nulo");
        Objects.requireNonNull(occurredOn, "OccurredOn não pode ser vazio/nulo.");
    }

    public static PixReconciledEvent of(String reconciliationId, EndToEndId endToEndId, TxId txId, Money amount) {
        return new PixReconciledEvent(reconciliationId, endToEndId, txId, amount, Instant.now());
    }
}

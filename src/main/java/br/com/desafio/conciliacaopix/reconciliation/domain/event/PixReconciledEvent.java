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
        Money amount,
        Instant occurredOn
) implements DomainEvent {

    public PixReconciledEvent {
        Objects.requireNonNull(reconciliationId, "ReconciliationId não pode ser vazio/nulo.");
        Objects.requireNonNull(endToEndId, "EndToEndId é obrigatório e não pode ser vazio/nulo.");
        Objects.requireNonNull(amount, "Amount não pode ser vazio/nulo");
        Objects.requireNonNull(occurredOn, "OcurredOn não pode ser vazio/nulo.");
    }

    public static PixReconciledEvent of(String reconciliationId, EndToEndId endToEndId, TxId txId, Money amount) {
        return new PixReconciledEvent(reconciliationId, endToEndId, txId, amount, Instant.now());
    }
}

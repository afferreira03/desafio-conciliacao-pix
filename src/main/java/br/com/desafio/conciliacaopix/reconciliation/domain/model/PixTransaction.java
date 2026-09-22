package br.com.desafio.conciliacaopix.reconciliation.domain.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public record PixTransaction(
        EndToEndId endToEndId,
        TxId txId,
        Money amount,
        Instant paymentTimestamp,
        String pixKey
) {

    public PixTransaction {
        Objects.requireNonNull(endToEndId, "");
        Objects.requireNonNull(amount, "");
        Objects.requireNonNull(paymentTimestamp, "");
    }

    public Optional<TxId> getTxId() {
        return Optional.ofNullable(this.txId);
    }

}

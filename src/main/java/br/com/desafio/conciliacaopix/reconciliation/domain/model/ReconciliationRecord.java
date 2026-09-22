package br.com.desafio.conciliacaopix.reconciliation.domain.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.*;
import com.fasterxml.uuid.Generators;
import lombok.Getter;

import java.time.Instant;
import java.util.Objects;

@Getter
public class ReconciliationRecord {
    private final String id;
    private final EndToEndId endToEndId;
    private final TxId txId;
    private final Money transactionAmount;
    private final Money expectedAmount;
    private final ReconciliationStatus status;
    private final InconsistencyReason inconsistencyReason;
    private final Instant createdAt;

    private ReconciliationRecord(String id, EndToEndId endToEndId, TxId txId, Money transactionAmount, Money expectedAmount, ReconciliationStatus status, InconsistencyReason inconsistencyReason, Instant createdAt) {
        this.id = Objects.requireNonNull(id, "ID não pode ser vazio/nulo.");
        this.endToEndId = Objects.requireNonNull(endToEndId, "EndToEndID é obrigatório e não deve ser nulo");
        this.txId = txId;
        this.transactionAmount = Objects.requireNonNull(transactionAmount, "Valor da transação é obrigatório.");
        this.expectedAmount = expectedAmount;
        this.status = Objects.requireNonNull(status, "Status da reconciliação é obrigatório");
        this.inconsistencyReason = inconsistencyReason;
        this.createdAt = Objects.requireNonNull(createdAt, "Data da transação é obrigatória.");
    }

    public static ReconciliationRecord createReconciled(EndToEndId endToEndId, TxId txId, Money amount) {
        return new ReconciliationRecord(
                Generators.timeBasedEpochGenerator().generate().toString(),
                endToEndId,
                txId,
                amount,
                amount,
                ReconciliationStatus.CONCILIADO, null,
                Instant.now()
        );
    }

    public static ReconciliationRecord createInconsistent(
            EndToEndId endToEndId,
            TxId txId,
            Money transactionAmount,
            Money expectedAmount,
            InconsistencyReason reason) {
        return new ReconciliationRecord(
                Generators.timeBasedEpochGenerator().generate().toString(),
                endToEndId,
                txId,
                transactionAmount,
                expectedAmount,
                ReconciliationStatus.INCONSISTENTE,
                reason,
                Instant.now()
        );
    }

    public static ReconciliationRecord createPending(EndToEndId endToEndId, TxId txId, Money transactionAmount) {
        return new ReconciliationRecord(
                Generators.timeBasedEpochGenerator().generate().toString(),
                endToEndId,
                txId,
                transactionAmount,
                null,
                ReconciliationStatus.PENDENTE,
                null,
                Instant.now()
        );
    }
}

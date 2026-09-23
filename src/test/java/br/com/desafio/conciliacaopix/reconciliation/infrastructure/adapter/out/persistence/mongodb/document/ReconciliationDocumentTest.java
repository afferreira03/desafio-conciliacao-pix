package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReconciliationDocumentTest {

    private static final EndToEndId E2E = EndToEndId.of("E0000000020260919123456789012345");

    @Test
    @DisplayName("Deve reconstruir conciliação PENDENTE sem txId e sem valor esperado")
    void shouldRestorePendingRecordWithoutTxIdAndExpectedAmount() {
        var original = ReconciliationRecord.createPending(E2E, null, Money.of(50.00));

        ReconciliationRecord restored = ReconciliationDocument.toDomain(ReconciliationDocument.fromDomain(original));

        assertThat(restored.getId()).isEqualTo(original.getId());
        assertThat(restored.getTxId()).isNull();
        assertThat(restored.getExpectedAmount()).isNull();
        assertThat(restored.getStatus()).isEqualTo(ReconciliationStatus.PENDENTE);
    }

    @Test
    @DisplayName("Deve reconstruir conciliação INCONSISTENTE preservando todos os campos")
    void shouldRestoreInconsistentRecordRoundTrip() {
        var original = ReconciliationRecord.createInconsistent(
                E2E, TxId.of("TX123"), Money.of(149.99), Money.of(150.00), InconsistencyReason.AMOUNT_MISMATCH);

        ReconciliationRecord restored = ReconciliationDocument.toDomain(ReconciliationDocument.fromDomain(original));

        assertThat(restored.getTxId()).isEqualTo(TxId.of("TX123"));
        assertThat(restored.getTransactionAmount()).isEqualTo(Money.of(149.99));
        assertThat(restored.getExpectedAmount()).isEqualTo(Money.of(150.00));
        assertThat(restored.getInconsistencyReason()).isEqualTo(InconsistencyReason.AMOUNT_MISMATCH);
        assertThat(restored.getCreatedAt()).isEqualTo(original.getCreatedAt());
    }
}

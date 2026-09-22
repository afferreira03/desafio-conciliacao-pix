package br.com.desafio.conciliacaopix.reconciliation.domain.service;

import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixInconsistentEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixPendingEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixReconciledEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.PixTransaction;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconciliationEngineTest {

    private ReconciliationEngine engine;

    @BeforeEach
    void setUp() {
        engine = new ReconciliationEngine();
    }

    private PixTransaction createTransaction(String e2e, String txId, double amount, Instant timestamp) {
        return new PixTransaction(
                EndToEndId.of(e2e),
                txId != null ? TxId.of(txId) : null,
                Money.of(amount),
                timestamp,
                "user@email.com"
        );
    }

    private Invoice createInvoice(String id, String txId, double amount, InvoiceStatus status, Instant expiration) {
        return new Invoice(
                id,
                TxId.of(txId),
                Money.of(amount),
                status,
                Instant.now().minus(1, ChronoUnit.HOURS),
                expiration
        );
    }


    @Test
    @DisplayName("Deve conciliar com sucesso quando transação e fatura conferem exatamente")
    void shouldReconcileSuccessfullyWhenInvoiceMatches() {
        var now = Instant.now();
        var tx = createTransaction("E0000000020260919123456789012345", "TX123", 150.00, now);
        var invoice = createInvoice("INV-1", "TX123", 150.00, InvoiceStatus.ABERTA, now.plus(1, ChronoUnit.DAYS));

        ReconciliationResult result = engine.reconcile(tx, Optional.of(invoice));

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.CONCILIADO);
        assertThat(result.record().getInconsistencyReason()).isNull();
        assertThat(result.updatedInvoice()).isPresent();
        assertThat(result.updatedInvoice().get().getStatus()).isEqualTo(InvoiceStatus.PAGA);
        assertThat(result.event()).isInstanceOf(PixReconciledEvent.class);
    }

    @Test
    @DisplayName("Deve marcar como pendente quando a fatura não for encontrada")
    void shouldReturnPendingWhenInvoiceIsNotFound() {
        var tx = createTransaction("E0000000020260919123456789012345", "TX999", 200.00, Instant.now());

        ReconciliationResult result = engine.reconcile(tx, Optional.empty());

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.PENDENTE);
        assertThat(result.updatedInvoice()).isEmpty();
        assertThat(result.event()).isInstanceOf(PixPendingEvent.class);
    }

    @Test
    @DisplayName("Deve acusar inconsistência quando o valor do Pix divergir da fatura")
    void shouldReturnInconsistentWhenAmountMismatches() {
        var now = Instant.now();
        var tx = createTransaction("E0000000020260919123456789012345", "TX123", 149.99, now);
        var invoice = createInvoice("INV-1", "TX123", 150.00, InvoiceStatus.ABERTA, now.plus(1, ChronoUnit.DAYS));

        ReconciliationResult result = engine.reconcile(tx, Optional.of(invoice));

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.INCONSISTENTE);
        assertThat(result.record().getInconsistencyReason()).isEqualTo(InconsistencyReason.AMOUNT_MISMATCH);
        assertThat(result.event()).isInstanceOf(PixInconsistentEvent.class);
    }

    @Test
    @DisplayName("Deve acusar inconsistência quando a fatura já estiver paga")
    void shouldReturnInconsistentWhenInvoiceAlreadyPaid() {
        var now = Instant.now();
        var tx = createTransaction("E0000000020260919123456789012345", "TX123", 150.00, now);
        var invoice = createInvoice("INV-1", "TX123", 150.00, InvoiceStatus.PAGA, now.plus(1, ChronoUnit.DAYS));

        ReconciliationResult result = engine.reconcile(tx, Optional.of(invoice));

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.INCONSISTENTE);
        assertThat(result.record().getInconsistencyReason()).isEqualTo(InconsistencyReason.INVOICE_ALREADY_PAID);
        assertThat(result.event()).isInstanceOf(PixInconsistentEvent.class);
    }

    @Test
    @DisplayName("Deve acusar inconsistência quando a fatura estiver expirada")
    void shouldReturnInconsistentWhenInvoiceIsExpired() {
        var now = Instant.now();
        var tx = createTransaction("E0000000020260919123456789012345", "TX123", 150.00, now);
        var expiredInvoice = createInvoice("INV-1", "TX123", 150.00, InvoiceStatus.ABERTA, now.minus(1, ChronoUnit.HOURS));

        ReconciliationResult result = engine.reconcile(tx, Optional.of(expiredInvoice));

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.INCONSISTENTE);
        assertThat(result.record().getInconsistencyReason()).isEqualTo(InconsistencyReason.INVOICE_EXPIRED);
        assertThat(result.event()).isInstanceOf(PixInconsistentEvent.class);
    }

    @Test
    @DisplayName("Deve acusar inconsistência quando a fatura estiver cancelada")
    void shouldReturnInconsistentWhenInvoiceIsCancelled() {
        var now = Instant.now();
        var tx = createTransaction("E0000000020260919123456789012345", "TX123", 150.00, now);
        var invoice = createInvoice("INV-1", "TX123", 150.00, InvoiceStatus.CANCELADA, now.plus(1, ChronoUnit.DAYS));

        ReconciliationResult result = engine.reconcile(tx, Optional.of(invoice));

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.INCONSISTENTE);
        assertThat(result.record().getInconsistencyReason()).isEqualTo(InconsistencyReason.INVOICE_CANCELLED);
        assertThat(result.event()).isInstanceOf(PixInconsistentEvent.class);
    }

    @Test
    @DisplayName("Deve acusar inconsistência quando a fatura já estiver com status EXPIRADA")
    void shouldReturnInconsistentWhenInvoiceStatusIsAlreadyExpired() {
        var now = Instant.now();
        var tx = createTransaction("E0000000020260919123456789012345", "TX123", 150.00, now);
        var invoice = createInvoice("INV-1", "TX123", 150.00, InvoiceStatus.EXPIRADA, now.plus(1, ChronoUnit.DAYS));

        ReconciliationResult result = engine.reconcile(tx, Optional.of(invoice));

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.INCONSISTENTE);
        assertThat(result.record().getInconsistencyReason()).isEqualTo(InconsistencyReason.INVOICE_EXPIRED);
        assertThat(result.event()).isInstanceOf(PixInconsistentEvent.class);
    }

    @Test
    @DisplayName("Deve permitir transação sem TxId e marcar como PENDENTE na ausência de fatura")
    void shouldAllowTransactionWithoutTxIdAndSetPending() {
        var tx = createTransaction("E0000000020260919123456789012345", null, 50.00, Instant.now());

        ReconciliationResult result = engine.reconcile(tx, Optional.empty());

        assertThat(result.record().getStatus()).isEqualTo(ReconciliationStatus.PENDENTE);
        assertThat(result.record().getTxId()).isNull();
    }

    @Test
    @DisplayName("Deve lançar NullPointerException se a transação ou optional da fatura forem nulos")
    void shouldThrowExceptionWhenArgumentsAreNull() {
        assertThatThrownBy(() -> engine.reconcile(null, Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("PixTransaction");

        var tx = createTransaction("E0000000020260919123456789012345", "TX123", 150.00, Instant.now());
        assertThatThrownBy(() -> engine.reconcile(tx, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("InvoiceOptional");
    }

}

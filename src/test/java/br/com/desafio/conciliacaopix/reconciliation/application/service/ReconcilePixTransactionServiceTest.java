package br.com.desafio.conciliacaopix.reconciliation.application.service;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconciliationPixCommand;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.DomainEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixInconsistentEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixPendingEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixReconciledEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.*;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReconcilePixTransactionServiceTest {

    private static final String E2E_ID = "E0000000020260919123456789012345";
    private static final String PIX_KEY = "user@email.com";

    @Mock
    private LoadInvoicePort loadInvoicePort;

    @Mock
    private SaveReconciliationPort saveReconciliationPort;

    @Mock
    private LoadReconciliationPort loadReconciliationPort;

    private ReconcilePixTransactionService service;

    @BeforeEach
    void setUp() {
        service = new ReconcilePixTransactionService(
                new ReconciliationEngine(),
                loadInvoicePort,
                saveReconciliationPort,
                loadReconciliationPort
        );
    }

    private ReconciliationPixCommand command(String txId, String pixKey, double amount) {
        return new ReconciliationPixCommand(E2E_ID, txId, Money.of(amount), Instant.now(), pixKey);
    }

    private Invoice openInvoice(String id, String txId, double amount) {
        return new Invoice(
                id,
                TxId.of(txId),
                Money.of(amount),
                InvoiceStatus.ABERTA,
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now().plus(1, ChronoUnit.DAYS)
        );
    }

    @Test
    @DisplayName("Deve retornar a conciliação existente sem reprocessar quando o endToEndId já foi processado")
    void shouldReturnExistingRecordWhenEndToEndIdAlreadyProcessed() {
        var existing = ReconciliationRecord.createPending(EndToEndId.of(E2E_ID), TxId.of("TX123"), Money.of(150.00));
        when(loadReconciliationPort.findByEndToEndId(EndToEndId.of(E2E_ID))).thenReturn(Optional.of(existing));

        ReconciliationRecord result = service.reconcile(command("TX123", PIX_KEY, 150.00));

        assertThat(result).isSameAs(existing);
        verifyNoInteractions(loadInvoicePort, saveReconciliationPort);
    }

    @Test
    @DisplayName("Deve conciliar pelo txId e persistir fatura PAGA com evento de conciliação")
    void shouldReconcileByTxIdAndPersistPaidInvoice() {
        when(loadReconciliationPort.findByEndToEndId(any())).thenReturn(Optional.empty());
        when(loadInvoicePort.findByTxId(TxId.of("TX123"))).thenReturn(Optional.of(openInvoice("INV-1", "TX123", 150.00)));

        ReconciliationRecord result = service.reconcile(command("TX123", PIX_KEY, 150.00));

        assertThat(result.getStatus()).isEqualTo(ReconciliationStatus.CONCILIADO);

        ArgumentCaptor<Optional<Invoice>> invoiceCaptor = ArgumentCaptor.captor();
        ArgumentCaptor<DomainEvent> eventCaptor = ArgumentCaptor.forClass(DomainEvent.class);
        verify(saveReconciliationPort).save(eq(result), invoiceCaptor.capture(), eventCaptor.capture());

        assertThat(invoiceCaptor.getValue()).hasValueSatisfying(invoice ->
                assertThat(invoice.getStatus()).isEqualTo(InvoiceStatus.PAGA));
        assertThat(eventCaptor.getValue()).isInstanceOf(PixReconciledEvent.class);
        verify(loadInvoicePort, never()).findPendingCandidatesByFallback(anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("Deve marcar como PENDENTE quando o txId não corresponde a nenhuma fatura")
    void shouldReturnPendingWhenTxIdHasNoInvoice() {
        when(loadReconciliationPort.findByEndToEndId(any())).thenReturn(Optional.empty());
        when(loadInvoicePort.findByTxId(any())).thenReturn(Optional.empty());

        ReconciliationRecord result = service.reconcile(command("TX999", PIX_KEY, 150.00));

        assertThat(result.getStatus()).isEqualTo(ReconciliationStatus.PENDENTE);
        verify(saveReconciliationPort).save(eq(result), eq(Optional.empty()), any(PixPendingEvent.class));
    }

    @Test
    @DisplayName("Deve conciliar pelo fallback (chave Pix + valor + janela) quando há exatamente uma candidata")
    void shouldReconcileByFallbackWhenSingleCandidate() {
        when(loadReconciliationPort.findByEndToEndId(any())).thenReturn(Optional.empty());
        when(loadInvoicePort.findPendingCandidatesByFallback(eq(PIX_KEY), eq(Money.of(150.00)), any(), any()))
                .thenReturn(List.of(openInvoice("INV-1", "TX123", 150.00)));

        ReconciliationRecord result = service.reconcile(command(null, PIX_KEY, 150.00));

        assertThat(result.getStatus()).isEqualTo(ReconciliationStatus.CONCILIADO);
        verify(loadInvoicePort, never()).findByTxId(any());
        verify(saveReconciliationPort).save(eq(result), any(), any(PixReconciledEvent.class));
    }

    @Test
    @DisplayName("Deve acusar inconsistência quando o fallback encontra múltiplas faturas candidatas")
    void shouldReturnInconsistentWhenFallbackMatchesMultipleInvoices() {
        when(loadReconciliationPort.findByEndToEndId(any())).thenReturn(Optional.empty());
        when(loadInvoicePort.findPendingCandidatesByFallback(eq(PIX_KEY), any(), any(), any()))
                .thenReturn(List.of(openInvoice("INV-1", "TX123", 150.00), openInvoice("INV-2", "TX456", 150.00)));

        ReconciliationRecord result = service.reconcile(command(null, PIX_KEY, 150.00));

        assertThat(result.getStatus()).isEqualTo(ReconciliationStatus.INCONSISTENTE);
        assertThat(result.getInconsistencyReason()).isEqualTo(InconsistencyReason.MULTIPLE_INVOICES_MATCHED);
        verify(saveReconciliationPort).save(eq(result), eq(Optional.empty()), any(PixInconsistentEvent.class));
    }

    @Test
    @DisplayName("Deve marcar como PENDENTE sem consultar faturas quando não há txId nem chave Pix")
    void shouldReturnPendingWhenNoTxIdAndNoPixKey() {
        when(loadReconciliationPort.findByEndToEndId(any())).thenReturn(Optional.empty());

        ReconciliationRecord result = service.reconcile(command(null, null, 150.00));

        assertThat(result.getStatus()).isEqualTo(ReconciliationStatus.PENDENTE);
        verifyNoInteractions(loadInvoicePort);
        verify(saveReconciliationPort).save(eq(result), eq(Optional.empty()), any(PixPendingEvent.class));
    }
}

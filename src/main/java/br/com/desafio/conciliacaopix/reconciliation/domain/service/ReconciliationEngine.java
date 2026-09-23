package br.com.desafio.conciliacaopix.reconciliation.domain.service;

import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixInconsistentEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixPendingEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixReconciledEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.PixTransaction;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;

import java.util.Objects;
import java.util.Optional;

public class ReconciliationEngine {

    public ReconciliationResult reconcile(PixTransaction pixTransaction, Optional<Invoice> invoiceOptional) {
        Objects.requireNonNull(pixTransaction, "PixTransaction não pode ser nulo.");
        Objects.requireNonNull(invoiceOptional, "InvoiceOptional não pode ser nulo.");

        if (invoiceOptional.isEmpty()) {
            ReconciliationRecord reconciliationRecord = ReconciliationRecord.createPending(
                    pixTransaction.endToEndId(),
                    pixTransaction.txId(),
                    pixTransaction.amount()
            );

            PixPendingEvent pixPendingEvent = PixPendingEvent.of(
                    reconciliationRecord.getId(),
                    reconciliationRecord.getEndToEndId(),
                    reconciliationRecord.getTxId(),
                    reconciliationRecord.getTransactionAmount()
            );
            return new ReconciliationResult(reconciliationRecord, Optional.empty(), pixPendingEvent);
        }

        Invoice invoice = invoiceOptional.get();

        if (invoice.getStatus() == InvoiceStatus.PAGA) {
            return createInconsistencyResult(pixTransaction, Optional.empty(), invoice.getAmount(), InconsistencyReason.INVOICE_ALREADY_PAID);
        }

        if (invoice.getStatus() == InvoiceStatus.ABERTA && invoice.isExpired(pixTransaction.paymentTimestamp())) {
            invoice.markAsExpired(pixTransaction.paymentTimestamp());
            return createInconsistencyResult(pixTransaction, Optional.of(invoice), invoice.getAmount(), InconsistencyReason.INVOICE_EXPIRED);
        }

        if (invoice.getStatus() == InvoiceStatus.EXPIRADA) {
            return createInconsistencyResult(pixTransaction, Optional.empty(), invoice.getAmount(), InconsistencyReason.INVOICE_EXPIRED);
        }

        if (invoice.getStatus() == InvoiceStatus.CANCELADA) {
            return createInconsistencyResult(pixTransaction, Optional.empty(), invoice.getAmount(), InconsistencyReason.INVOICE_CANCELLED);
        }

        if (!pixTransaction.amount().isEqualTo(invoice.getAmount())) {
            return createInconsistencyResult(pixTransaction, Optional.empty(), invoice.getAmount(), InconsistencyReason.AMOUNT_MISMATCH);
        }

        invoice.markAsPaid();
        ReconciliationRecord reconciliationRecord = ReconciliationRecord.createReconciled(
                pixTransaction.endToEndId(),
                pixTransaction.txId(),
                pixTransaction.amount()
        );

        PixReconciledEvent event = PixReconciledEvent.of(
                reconciliationRecord.getId(),
                reconciliationRecord.getEndToEndId(),
                reconciliationRecord.getTxId(),
                reconciliationRecord.getTransactionAmount()
        );

        return new ReconciliationResult(reconciliationRecord, Optional.of(invoice), event);
    }

    private ReconciliationResult createInconsistencyResult(PixTransaction pixTransaction, Optional<Invoice> invoice, Money expectedAmount, InconsistencyReason reason) {
        ReconciliationRecord reconciliationRecord = ReconciliationRecord.createInconsistent(
                pixTransaction.endToEndId(),
                pixTransaction.txId(),
                pixTransaction.amount(),
                expectedAmount,
                reason
        );

        PixInconsistentEvent pixInconsistentEvent = PixInconsistentEvent.of(
                reconciliationRecord.getId(),
                reconciliationRecord.getEndToEndId(),
                reconciliationRecord.getTxId(),
                reconciliationRecord.getTransactionAmount(),
                reconciliationRecord.getExpectedAmount(),
                reason
        );
        return new ReconciliationResult(reconciliationRecord, invoice, pixInconsistentEvent);
    }
}

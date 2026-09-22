package br.com.desafio.conciliacaopix.reconciliation.application.service;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconcilePixTransactionUseCase;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconciliationPixCommand;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixInconsistentEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.PixTransaction;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationEngine;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationResult;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ReconcilePixTransactionService implements ReconcilePixTransactionUseCase {

    private final ReconciliationEngine reconciliationEngine;
    private final LoadInvoicePort loadInvoicePort;
    private final SaveReconciliationPort saveReconciliationPort;

    public ReconcilePixTransactionService(ReconciliationEngine reconciliationEngine, LoadInvoicePort loadInvoicePort, SaveReconciliationPort saveReconciliationPort) {
        this.reconciliationEngine = Objects.requireNonNull(reconciliationEngine, "ReconciliationEngine é obrigatório.");
        this.loadInvoicePort = Objects.requireNonNull(loadInvoicePort, "LoadInvoicePort é obrigatório.");
        this.saveReconciliationPort = Objects.requireNonNull(saveReconciliationPort, "SaveReconciliationPort é obrigatório.");
    }

    @Override
    public ReconciliationRecord reconcile(ReconciliationPixCommand command) {
        Objects.requireNonNull(command, "Command não pode ser vazio/nulo.");
        PixTransaction transaction = command.toDomain();

        Optional<Invoice> invoiceOptional;

        if (transaction.getTxId().isPresent()) {
            invoiceOptional = loadInvoicePort.findByTxId(transaction.getTxId().get());
            ReconciliationResult result = reconciliationEngine.reconcile(transaction, invoiceOptional);
            return persist(result);
        }

        if (transaction.pixKey() != null) {
            List<Invoice> candidates = loadInvoicePort.findPendingCandidatesByFallback(
                    transaction.pixKey(),
                    transaction.amount(),
                    transaction.paymentTimestamp(),
                    Duration.ofMinutes(30)
            );

            if (candidates.size() == 1) {
                ReconciliationResult result = reconciliationEngine.reconcile(transaction, Optional.of(candidates.getFirst()));
                return persist(result);
            }

            if (candidates.size() > 1) {
                ReconciliationRecord reconciliationRecord = ReconciliationRecord.createInconsistent(
                        transaction.endToEndId(),
                        null,
                        transaction.amount(),
                        transaction.amount(),
                        InconsistencyReason.MULTIPLE_INVOICES_MATCHED
                );

                PixInconsistentEvent event = PixInconsistentEvent.of(
                        reconciliationRecord.getId(),
                        reconciliationRecord.getEndToEndId(),
                        reconciliationRecord.getTxId(),
                        reconciliationRecord.getTransactionAmount(),
                        reconciliationRecord.getExpectedAmount(),
                        InconsistencyReason.MULTIPLE_INVOICES_MATCHED
                );
                return persist(new ReconciliationResult(reconciliationRecord, Optional.empty(), event));
            }
        }

        ReconciliationResult reconciliationResult = reconciliationEngine.reconcile(transaction, Optional.empty());
        return persist(reconciliationResult);
    }

    private ReconciliationRecord persist(ReconciliationResult result) {
        saveReconciliationPort.save(result.record(), result.updatedInvoice(), result.event());
        return result.record();
    }
}

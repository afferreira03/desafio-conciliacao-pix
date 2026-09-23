package br.com.desafio.conciliacaopix.reconciliation.application.service;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconcilePixTransactionUseCase;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconciliationPixCommand;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.ReconciliationMetricsPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixInconsistentEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.PixTransaction;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationEngine;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ReconcilePixTransactionService implements ReconcilePixTransactionUseCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReconcilePixTransactionService.class);

    private final ReconciliationEngine reconciliationEngine;
    private final LoadInvoicePort loadInvoicePort;
    private final SaveReconciliationPort saveReconciliationPort;
    private final LoadReconciliationPort loadReconciliationPort;
    private final ReconciliationMetricsPort metricsPort;

    public ReconcilePixTransactionService(ReconciliationEngine reconciliationEngine, LoadInvoicePort loadInvoicePort, SaveReconciliationPort saveReconciliationPort, LoadReconciliationPort loadReconciliationPort, ReconciliationMetricsPort metricsPort) {
        this.reconciliationEngine = Objects.requireNonNull(reconciliationEngine, "ReconciliationEngine é obrigatório.");
        this.loadInvoicePort = Objects.requireNonNull(loadInvoicePort, "LoadInvoicePort é obrigatório.");
        this.saveReconciliationPort = Objects.requireNonNull(saveReconciliationPort, "SaveReconciliationPort é obrigatório.");
        this.loadReconciliationPort = Objects.requireNonNull(loadReconciliationPort, "LoadReconciliationPort é obrigatório");
        this.metricsPort = Objects.requireNonNull(metricsPort, "ReconciliationMetricsPort é obrigatório.");
    }

    @Override
    public ReconciliationRecord reconcile(ReconciliationPixCommand command) {
        Objects.requireNonNull(command, "Command não pode ser vazio/nulo.");
        PixTransaction transaction = command.toDomain();

        Optional<ReconciliationRecord> existing = loadReconciliationPort.findByEndToEndId(transaction.endToEndId());
        if (existing.isPresent()) {
            LOG.info("Reconcilicação já existe {}.", existing.get().getEndToEndId());
            metricsPort.recordDuplicate();
            return existing.get();
        }

        Optional<Invoice> invoiceOptional;

        if (transaction.getTxId().isPresent()) {
            invoiceOptional = loadInvoicePort.findByTxId(transaction.getTxId().get());
            ReconciliationResult result = reconciliationEngine.reconcile(transaction, invoiceOptional);
            return persist(result, transaction);
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
                return persist(result, transaction);
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
                return persist(new ReconciliationResult(reconciliationRecord, Optional.empty(), event), transaction);
            }
        }

        ReconciliationResult reconciliationResult = reconciliationEngine.reconcile(transaction, Optional.empty());
        return persist(reconciliationResult, transaction);
    }

    private ReconciliationRecord persist(ReconciliationResult result, PixTransaction transaction) {
        saveReconciliationPort.save(result.record(), result.updatedInvoice(), result.event());
        // Após o save (transacional) retornar: só mede o que foi efetivamente commitado.
        metricsPort.recordReconciled(result.record(), transaction.paymentTimestamp());
        return result.record();
    }
}

package br.com.desafio.conciliacaopix.reconciliation.application.port.in;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;

public interface ReconcilePixTransactionUseCase {
    ReconciliationRecord reconcile(ReconciliationPixCommand command);
}

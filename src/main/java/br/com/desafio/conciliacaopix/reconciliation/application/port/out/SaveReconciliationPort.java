package br.com.desafio.conciliacaopix.reconciliation.application.port.out;

import br.com.desafio.conciliacaopix.reconciliation.domain.event.DomainEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;

import java.util.Optional;

public interface SaveReconciliationPort {
    void save(ReconciliationRecord reconciliationRecord, Optional<Invoice> updatesInvoice, DomainEvent domainEvent);
}

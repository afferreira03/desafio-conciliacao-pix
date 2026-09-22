package br.com.desafio.conciliacaopix.reconciliation.domain.service;

import br.com.desafio.conciliacaopix.reconciliation.domain.event.DomainEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;

import java.util.Optional;

public record ReconciliationResult(
        ReconciliationRecord record,
        Optional<Invoice> updatedInvoice,
        DomainEvent event
) {
}

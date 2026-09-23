package br.com.desafio.conciliacaopix.reconciliation.application.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;

import java.time.Instant;

/**
 * Filtros opcionais para a consulta de conciliações. Campos nulos não filtram.
 */
public record ReconciliationSearchCriteria(
        ReconciliationStatus status,
        InconsistencyReason reason,
        Instant from,
        Instant to
) {
    public ReconciliationSearchCriteria {
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("O início do período ('from') deve ser anterior ao fim ('to').");
        }
    }
}

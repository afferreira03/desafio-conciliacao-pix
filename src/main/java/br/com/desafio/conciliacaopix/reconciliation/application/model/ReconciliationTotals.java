package br.com.desafio.conciliacaopix.reconciliation.application.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;

import java.util.Objects;

/**
 * Linha agregada (status + motivo) devolvida pela persistência para montar o relatório.
 * {@code reason} é nulo para status sem motivo de inconsistência.
 */
public record ReconciliationTotals(
        ReconciliationStatus status,
        InconsistencyReason reason,
        long count,
        Money total
) {
    public ReconciliationTotals {
        Objects.requireNonNull(status, "Status é obrigatório.");
        Objects.requireNonNull(total, "Total é obrigatório.");
    }
}

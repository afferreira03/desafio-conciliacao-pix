package br.com.desafio.conciliacaopix.reconciliation.application.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;

import java.time.Instant;
import java.util.List;

/**
 * Relatório simples de conciliação: totais por status e, para INCONSISTENTE, por motivo.
 */
public record ReconciliationSummary(
        Instant from,
        Instant to,
        long totalCount,
        Money totalAmount,
        List<StatusSummary> statuses
) {
    public ReconciliationSummary {
        statuses = List.copyOf(statuses);
    }

    public record StatusSummary(ReconciliationStatus status, long count, Money total, List<ReasonSummary> reasons) {
        public StatusSummary {
            reasons = List.copyOf(reasons);
        }
    }

    public record ReasonSummary(InconsistencyReason reason, long count, Money total) {
    }
}

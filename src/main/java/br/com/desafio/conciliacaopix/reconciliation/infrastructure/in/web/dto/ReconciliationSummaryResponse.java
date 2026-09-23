package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto;

import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Schema(name = "ReconciliationSummary", description = "Relatório de conciliação: totais por status e, para INCONSISTENTE, por motivo.")
public record ReconciliationSummaryResponse(
        @Schema(example = "2026-09-23T00:00:00Z", nullable = true) Instant from,
        @Schema(example = "2026-09-23T23:59:59Z", nullable = true) Instant to,
        @Schema(example = "5000") long totalCount,
        @Schema(example = "752340.15") BigDecimal totalAmount,
        List<StatusSummaryResponse> statuses
) {
    public static ReconciliationSummaryResponse from(ReconciliationSummary summary) {
        return new ReconciliationSummaryResponse(
                summary.from(),
                summary.to(),
                summary.totalCount(),
                summary.totalAmount().value(),
                summary.statuses().stream().map(StatusSummaryResponse::from).toList()
        );
    }

    @Schema(name = "StatusSummary")
    public record StatusSummaryResponse(
            @Schema(example = "INCONSISTENTE") ReconciliationStatus status,
            @Schema(example = "812") long count,
            @Schema(example = "120450.30") BigDecimal total,
            List<ReasonSummaryResponse> reasons
    ) {
        static StatusSummaryResponse from(ReconciliationSummary.StatusSummary status) {
            return new StatusSummaryResponse(
                    status.status(),
                    status.count(),
                    status.total().value(),
                    status.reasons().stream().map(ReasonSummaryResponse::from).toList()
            );
        }
    }

    @Schema(name = "ReasonSummary")
    public record ReasonSummaryResponse(
            @Schema(example = "AMOUNT_MISMATCH") InconsistencyReason reason,
            @Schema(example = "Valores da operação e da fatura não correspondem.") String description,
            @Schema(example = "510") long count,
            @Schema(example = "75210.00") BigDecimal total
    ) {
        static ReasonSummaryResponse from(ReconciliationSummary.ReasonSummary reason) {
            return new ReasonSummaryResponse(
                    reason.reason(),
                    reason.reason().getErrorMessage(),
                    reason.count(),
                    reason.total().value()
            );
        }
    }
}

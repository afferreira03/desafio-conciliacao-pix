package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.time.Instant;

@Schema(name = "Reconciliation", description = "Resultado da conciliação de uma transação Pix.")
public record ReconciliationResponse(
        @Schema(example = "0198f3a2-7c1e-7b4a-9d2e-3f6a1b2c4d5e") String id,
        @Schema(example = "E0000000020260919123456789012345") String endToEndId,
        @Schema(example = "TX123", nullable = true) String txId,
        @Schema(example = "CONCILIADO") ReconciliationStatus status,
        @Schema(example = "AMOUNT_MISMATCH", nullable = true) InconsistencyReason inconsistencyReason,
        @Schema(example = "Valores da operação e da fatura não correspondem.", nullable = true) String inconsistencyDescription,
        @Schema(example = "150.00") BigDecimal transactionAmount,
        @Schema(example = "150.00", nullable = true) BigDecimal expectedAmount,
        @Schema(example = "2026-09-23T13:45:10.123Z") Instant createdAt
) {
    public static ReconciliationResponse from(ReconciliationRecord record) {
        InconsistencyReason reason = record.getInconsistencyReason();
        return new ReconciliationResponse(
                record.getId(),
                record.getEndToEndId().value(),
                record.getTxId() != null ? record.getTxId().value() : null,
                record.getStatus(),
                reason,
                reason != null ? reason.getErrorMessage() : null,
                record.getTransactionAmount().value(),
                record.getExpectedAmount() != null ? record.getExpectedAmount().value() : null,
                record.getCreatedAt()
        );
    }
}

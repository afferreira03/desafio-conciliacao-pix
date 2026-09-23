package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto;

import br.com.desafio.conciliacaopix.reconciliation.application.model.InvoiceView;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.PixKeyMasker;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.time.Instant;

@Schema(name = "Invoice", description = "Fatura (cobrança Pix). A chave Pix é mascarada (LGPD).")
public record InvoiceResponse(
        @Schema(example = "TX123") String txId,
        @Schema(example = "150.00") BigDecimal amount,
        @Schema(example = "ABERTA") InvoiceStatus status,
        @Schema(example = "use***@email.com") String pixKey,
        @Schema(example = "2026-09-23T13:40:00Z") Instant createdAt,
        @Schema(example = "2026-12-31T23:59:59Z") Instant expiresAt
) {
    public static InvoiceResponse from(InvoiceView view) {
        return new InvoiceResponse(
                view.txId().value(),
                view.amount().value(),
                view.status(),
                PixKeyMasker.mask(view.pixKey()),
                view.createdAt(),
                view.expiresAt()
        );
    }
}

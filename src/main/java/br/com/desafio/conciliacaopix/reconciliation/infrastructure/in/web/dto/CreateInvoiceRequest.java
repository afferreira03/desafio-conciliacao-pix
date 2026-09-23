package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.CreateInvoiceCommand;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;

@Schema(name = "CreateInvoiceRequest", description = "Dados para abertura de uma fatura (cobrança Pix).")
public record CreateInvoiceRequest(
        @Schema(example = "TX123", description = "Identificador da cobrança Pix (1 a 35 caracteres alfanuméricos).")
        @NotBlank
        @Pattern(regexp = "^[a-zA-Z0-9]{1,35}$", message = "deve ter de 1 a 35 caracteres alfanuméricos")
        String txId,

        @Schema(example = "150.00")
        @NotNull
        @Positive
        @Digits(integer = 15, fraction = 2)
        BigDecimal amount,

        @Schema(example = "user@email.com", description = "Chave Pix recebedora (usada na conciliação por fallback).")
        @NotBlank
        @Size(max = 77)
        String pixKey,

        @Schema(example = "2026-12-31T23:59:59Z")
        @NotNull
        @Future
        Instant expiresAt
) {
    public CreateInvoiceCommand toCommand() {
        return new CreateInvoiceCommand(txId, Money.of(amount), pixKey, expiresAt);
    }
}

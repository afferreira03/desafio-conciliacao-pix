package br.com.desafio.conciliacaopix.reconciliation.application.port.in;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;

import java.time.Instant;
import java.util.Objects;

public record CreateInvoiceCommand(
        String txId,
        Money amount,
        String pixKey,
        Instant expiresAt
) {
    public CreateInvoiceCommand {
        Objects.requireNonNull(txId, "TxId é obrigatório.");
        Objects.requireNonNull(amount, "Amount é obrigatório.");
        Objects.requireNonNull(pixKey, "PixKey é obrigatória.");
        Objects.requireNonNull(expiresAt, "ExpiresAt é obrigatório.");
    }
}

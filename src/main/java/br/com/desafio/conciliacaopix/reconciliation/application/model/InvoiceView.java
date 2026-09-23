package br.com.desafio.conciliacaopix.reconciliation.application.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.time.Instant;

/**
 * Modelo de leitura da fatura. Contém a chave Pix (dado pessoal) — o mascaramento
 * é responsabilidade da borda que expõe o dado (API).
 */
public record InvoiceView(
        TxId txId,
        Money amount,
        InvoiceStatus status,
        String pixKey,
        Instant createdAt,
        Instant expiresAt
) {
}

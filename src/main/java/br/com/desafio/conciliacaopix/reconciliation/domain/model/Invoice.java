package br.com.desafio.conciliacaopix.reconciliation.domain.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import lombok.Getter;

import java.time.Instant;
import java.util.Objects;

@Getter
public class Invoice {
    private final String id;
    private final TxId txId;
    private final Money amount;
    private final Instant createdAt;
    private final Instant expirationDate;
    private InvoiceStatus status;

    public Invoice(String id, TxId txId, Money amount, InvoiceStatus status, Instant createdAt, Instant expirationDate) {
        this.id = Objects.requireNonNull(id, "Id cannot be null");
        this.txId = Objects.requireNonNull(txId, "TxId cannot be null");
        this.amount = Objects.requireNonNull(amount, "Amount cannot be null");
        this.status = Objects.requireNonNull(status, "Status cannot be null");
        this.createdAt = Objects.requireNonNull(createdAt, "CreatedAt cannot be null");
        this.expirationDate = Objects.requireNonNull(expirationDate, "ExpirationDate cannot be null");
    }

    public boolean isExpired(Instant reference) {
        return reference.isAfter(this.expirationDate);
    }

    public boolean isOpen() {
        return this.status == InvoiceStatus.ABERTA;
    }

    public void markAsExpired(Instant reference) {
        if (reference.isAfter(this.expirationDate)) {
            this.status = InvoiceStatus.EXPIRADA;
        }
    }

    public void markAsPaid() {
        if (!this.isOpen()) {
            throw new IllegalStateException("Fatura não está aberta e não pode ser paga.");
        }
        this.status = InvoiceStatus.PAGA;
    }

    public void cancel() {
        this.status = InvoiceStatus.CANCELADA;
    }
}

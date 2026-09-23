package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

@Document(collection = "invoices")
@CompoundIndex(name = "idx_fallback_lookup", def = "{'pixKey': 1, 'transactionAmount': 1, 'createdAt' : 1}")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InvoiceDocument {

    public static final String STATUS_FIELD_NAME = "status";

    @Id
    private String id;

    @Indexed(unique = true)
    private String txId;

    private String pixKey;
    private BigDecimal amount;
    private InvoiceStatus status;
    private Instant createdAt;
    private Instant expiryDate;

    public static InvoiceDocument fromDomain(Invoice invoice, String pixKey) {
        return new InvoiceDocument(
                invoice.getId(),
                invoice.getTxId().value(),
                pixKey,
                invoice.getAmount().value(),
                invoice.getStatus(),
                invoice.getCreatedAt(),
                invoice.getExpirationDate()
        );
    }

    public Invoice toDomain() {
        return new Invoice(
                this.id,
                TxId.of(this.txId),
                Money.of(this.amount),
                this.status,
                this.createdAt,
                this.expiryDate
        );
    }
}

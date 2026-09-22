package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

@Document(collection = "reconciliations")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ReconciliationDocument {

    @Id
    private String id;

    @Indexed(unique = true)
    private String endToEndId;

    private String txId;
    private BigDecimal transactionAmount;
    private BigDecimal expectedAmount;
    private ReconciliationStatus status;
    private InconsistencyReason inconsistencyReason;
    private Instant createdAt;

    public static ReconciliationDocument fromDomain(ReconciliationRecord reconciliationRecord) {
        return new ReconciliationDocument(
                reconciliationRecord.getId(),
                reconciliationRecord.getEndToEndId().value(),
                reconciliationRecord.getTxId() != null ? reconciliationRecord.getTxId().value() : null,
                reconciliationRecord.getTransactionAmount().value(),
                reconciliationRecord.getExpectedAmount() != null ? reconciliationRecord.getExpectedAmount().value() : null,
                reconciliationRecord.getStatus(),
                reconciliationRecord.getInconsistencyReason(),
                reconciliationRecord.getCreatedAt()
        );
    }

}

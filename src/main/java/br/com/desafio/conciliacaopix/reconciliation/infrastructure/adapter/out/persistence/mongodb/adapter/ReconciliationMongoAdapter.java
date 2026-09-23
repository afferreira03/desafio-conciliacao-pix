package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.adapter;

import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.DomainEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.InvoiceDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.ReconciliationDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataOutboxRepository;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataReconciliationRepository;
import com.mongodb.client.result.UpdateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;

@Component
public class ReconciliationMongoAdapter implements SaveReconciliationPort, LoadReconciliationPort {

    private final SpringDataReconciliationRepository reconciliationRepository;
    private final SpringDataOutboxRepository outboxRepository;
    private final MongoTemplate mongoTemplate;
    private final ObjectMapper objectMapper;

    public ReconciliationMongoAdapter(SpringDataReconciliationRepository reconciliationRepository, SpringDataOutboxRepository outboxRepository, MongoTemplate mongoTemplate, ObjectMapper objectMapper) {
        this.reconciliationRepository = reconciliationRepository;
        this.outboxRepository = outboxRepository;
        this.mongoTemplate = mongoTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    public void save(ReconciliationRecord reconciliationRecord, Optional<Invoice> updatedInvoice, DomainEvent domainEvent) {

        reconciliationRepository.save(ReconciliationDocument.fromDomain(reconciliationRecord));

        if (updatedInvoice.isPresent()) {
            Invoice invoice = updatedInvoice.get();
            UpdateResult result = mongoTemplate.updateFirst(
                    Query.query(Criteria
                            .where("_id").is(invoice.getId())
                            .and(InvoiceDocument.STATUS_FIELD_NAME).is(InvoiceStatus.ABERTA)),
                    Update.update(InvoiceDocument.STATUS_FIELD_NAME, invoice.getStatus()),
                    InvoiceDocument.class);
            if (result.getMatchedCount() == 0) {
                throw new OptimisticLockingFailureException(String.format("Invoice %s não está ABERTA.", invoice.getId()));
            }
        }

        String payload = objectMapper.writeValueAsString(domainEvent);
        outboxRepository.save(OutboxEventDocument.fromDomain(domainEvent, payload));
    }

    @Override
    public Optional<ReconciliationRecord> findByEndToEndId(EndToEndId endToEndId) {
        Optional<ReconciliationDocument> result = reconciliationRepository.findByEndToEndId(endToEndId.value());
        return result.map(ReconciliationDocument::toDomain);
    }
}

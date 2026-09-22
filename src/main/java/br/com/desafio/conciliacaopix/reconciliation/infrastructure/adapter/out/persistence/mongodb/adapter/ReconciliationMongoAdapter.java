package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.adapter;

import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.DomainEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.ReconciliationDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataInvoiceRepository;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataOutboxRepository;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataReconciliationRepository;
import org.springframework.dao.DuplicateKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;

@Component
public class ReconciliationMongoAdapter implements SaveReconciliationPort {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationMongoAdapter.class);

    private final SpringDataReconciliationRepository reconciliationRepository;
    private final SpringDataInvoiceRepository invoiceRepository;
    private final SpringDataOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public ReconciliationMongoAdapter(SpringDataReconciliationRepository reconciliationRepository, SpringDataInvoiceRepository invoiceRepository, SpringDataOutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.reconciliationRepository = reconciliationRepository;
        this.invoiceRepository = invoiceRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    public void save(ReconciliationRecord reconciliationRecord, Optional<Invoice> updatesInvoice, DomainEvent domainEvent) {
        try {
            reconciliationRepository.save(ReconciliationDocument.fromDomain(reconciliationRecord));

            updatesInvoice.ifPresent(invoice -> {
               invoiceRepository.findById(invoice.getId()).ifPresent(doc -> {
                   doc.setStatus(invoice.getStatus());
                   invoiceRepository.save(doc);
               });
            });

            String payload = objectMapper.writeValueAsString(domainEvent);
            outboxRepository.save(OutboxEventDocument.fromDomain(domainEvent,payload));

        } catch (DuplicateKeyException e) {
            log.warn("Transação Pix duplicada detectada (idempotência): {}. Ignorando inserção.", reconciliationRecord.getEndToEndId().value());
        }
    }
}

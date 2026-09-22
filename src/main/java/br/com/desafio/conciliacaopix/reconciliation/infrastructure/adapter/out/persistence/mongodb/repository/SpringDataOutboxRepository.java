package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface SpringDataOutboxRepository extends MongoRepository<OutboxEventDocument, String> {
    List<OutboxEventDocument> findByStatus(OutboxEventStatus status);
}

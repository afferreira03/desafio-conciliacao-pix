package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.ReconciliationDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface SpringDataReconciliationRepository extends MongoRepository<ReconciliationDocument, String> {
    Optional<ReconciliationDocument> findByEndToEndId(String endToEndId);
}

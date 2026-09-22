package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.InvoiceDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface SpringDataInvoiceRepository extends MongoRepository<InvoiceDocument, String> {

    Optional<InvoiceDocument> findByTxId(String txId);

    @Query("{'pixKey': ?0, 'amount': ?1, 'status': ?2, 'createdAt': { $gte:  ?3, $lte:  ?4} }")
    List<InvoiceDocument> findCandidatesFallback(
            String pixKey,
            BigDecimal amount,
            InvoiceStatus status,
            Instant startWindow,
            Instant endWindow
    );
}

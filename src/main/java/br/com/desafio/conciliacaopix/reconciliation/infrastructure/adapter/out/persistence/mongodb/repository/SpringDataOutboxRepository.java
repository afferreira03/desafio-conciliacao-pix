package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import org.springframework.data.domain.Limit;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface SpringDataOutboxRepository extends MongoRepository<OutboxEventDocument, String> {

    /**
     * Lote dos eventos mais antigos com o status informado (FIFO), limitado para não carregar
     * todo o backlog em memória de uma vez. Coberto pelo índice parcial {status, createdAt}.
     */
    List<OutboxEventDocument> findByStatusOrderByCreatedAtAsc(OutboxEventStatus status, Limit limit);
}

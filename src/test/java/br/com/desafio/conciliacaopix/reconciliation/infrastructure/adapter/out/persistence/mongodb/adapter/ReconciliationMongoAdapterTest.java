package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.adapter;

import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixPendingEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixReconciledEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.*;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.InvoiceDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.ReconciliationDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataOutboxRepository;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataReconciliationRepository;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifica a lógica do compare-and-set da fatura. O rollback real da transação só é
 * comprovável contra um Mongo de verdade (teste de integração).
 */
@ExtendWith(MockitoExtension.class)
class ReconciliationMongoAdapterTest {

    private static final EndToEndId E2E = EndToEndId.of("E0000000020260919123456789012345");

    @Mock
    private SpringDataReconciliationRepository reconciliationRepository;

    @Mock
    private SpringDataOutboxRepository outboxRepository;

    @Mock
    private MongoTemplate mongoTemplate;

    private ReconciliationMongoAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new ReconciliationMongoAdapter(reconciliationRepository, outboxRepository, mongoTemplate, JsonMapper.builder().build());
    }

    private Invoice paidInvoice() {
        var invoice = new Invoice("INV-1", TxId.of("TX123"), Money.of(150.00), InvoiceStatus.ABERTA,
                Instant.now().minus(1, ChronoUnit.HOURS), Instant.now().plus(1, ChronoUnit.DAYS));
        invoice.markAsPaid();
        return invoice;
    }

    private ReconciliationRecord reconciledRecord() {
        return ReconciliationRecord.createReconciled(E2E, TxId.of("TX123"), Money.of(150.00));
    }

    private PixReconciledEvent reconciledEvent(ReconciliationRecord record) {
        return PixReconciledEvent.of(record.getId(), record.getEndToEndId(), record.getTxId(), record.getTransactionAmount());
    }

    @Test
    @DisplayName("Deve atualizar a fatura condicionada a status ABERTA e gravar registro e outbox")
    void shouldConditionallyUpdateInvoiceAndSaveOutbox() {
        var record = reconciledRecord();
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(InvoiceDocument.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        adapter.save(record, Optional.of(paidInvoice()), reconciledEvent(record));

        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(query.capture(), update.capture(), eq(InvoiceDocument.class));

        Document filter = query.getValue().getQueryObject();
        assertThat(filter.get("_id")).isEqualTo("INV-1");
        assertThat(filter.get("status")).isEqualTo(InvoiceStatus.ABERTA);
        assertThat(update.getValue().getUpdateObject().get("$set", Document.class).get("status")).isEqualTo(InvoiceStatus.PAGA);

        verify(reconciliationRepository).save(any(ReconciliationDocument.class));
        ArgumentCaptor<OutboxEventDocument> outbox = ArgumentCaptor.forClass(OutboxEventDocument.class);
        verify(outboxRepository).save(outbox.capture());
        assertThat(outbox.getValue().getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(outbox.getValue().getEventType()).isEqualTo("PixReconciledEvent");
        assertThat(outbox.getValue().getPayload()).contains(E2E.value());
    }

    @Test
    @DisplayName("Deve lançar OptimisticLockingFailureException e não gravar outbox quando a fatura não está mais ABERTA")
    void shouldFailWhenInvoiceIsNoLongerOpen() {
        var record = reconciledRecord();
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(InvoiceDocument.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));

        assertThatThrownBy(() -> adapter.save(record, Optional.of(paidInvoice()), reconciledEvent(record)))
                .isInstanceOf(OptimisticLockingFailureException.class);

        verify(outboxRepository, never()).save(any());
    }

    @Test
    @DisplayName("Sem mudança de estado da fatura, não deve tocá-la, mas grava registro e outbox")
    void shouldNotTouchInvoiceWhenNotUpdated() {
        var record = ReconciliationRecord.createPending(E2E, null, Money.of(50.00));
        var event = PixPendingEvent.of(record.getId(), record.getEndToEndId(), null, record.getTransactionAmount());

        adapter.save(record, Optional.empty(), event);

        verifyNoInteractions(mongoTemplate);
        verify(reconciliationRepository).save(any(ReconciliationDocument.class));
        verify(outboxRepository).save(any(OutboxEventDocument.class));
    }
}

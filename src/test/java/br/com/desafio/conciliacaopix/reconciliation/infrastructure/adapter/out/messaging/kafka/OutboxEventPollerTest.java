package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.messaging.kafka;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataOutboxRepository;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.KafkaTopicsProperties;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.SchedulerTimeKnobs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEventPollerTest {

    private static final String RESULT_TOPIC = "pix.reconciliation.result";
    private static final int BATCH_SIZE = 3;

    @Mock
    private SpringDataOutboxRepository repository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxEventPoller poller;

    @BeforeEach
    void setUp() {
        var knobs = new SchedulerTimeKnobs(200, 1000, BATCH_SIZE);
        var topics = new KafkaTopicsProperties("pix.transactions", "pix.transactions.DLT", RESULT_TOPIC);
        poller = new OutboxEventPoller(knobs, topics, repository, kafkaTemplate);
    }

    private OutboxEventDocument pendingEvent(String id) {
        return OutboxEventDocument.builder()
                .id(id)
                .endToEndId("E0000000020260919123456789" + id)
                .eventType("PixReconciledEvent")
                .payload("{\"id\":\"" + id + "\"}")
                .status(OutboxEventStatus.PENDING)
                .createdAt(Instant.now())
                .build();
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<SendResult<String, String>> success() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }

    private CompletableFuture<SendResult<String, String>> failure() {
        return CompletableFuture.failedFuture(new RuntimeException("broker indisponível"));
    }

    @SuppressWarnings("unchecked")
    private List<OutboxEventDocument> savedEvents() {
        ArgumentCaptor<Collection<OutboxEventDocument>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(repository).saveAll(captor.capture());
        return new ArrayList<>(captor.getValue());
    }

    @Test
    @DisplayName("Não deve publicar nem salvar nada quando não há eventos pendentes")
    void shouldDoNothingWhenNoPendingEvents() {
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class))).thenReturn(List.of());

        poller.poll();

        verifyNoInteractions(kafkaTemplate);
        verify(repository, never()).saveAll(any());
    }

    @Test
    @DisplayName("Deve publicar com endToEndId como chave e marcar todos como SENT quando todos os envios têm sucesso")
    void shouldMarkAllAsSentWhenAllSendsSucceed() {
        var events = List.of(pendingEvent("001001"), pendingEvent("001002"));
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class))).thenReturn(events);
        when(kafkaTemplate.send(eq(RESULT_TOPIC), anyString(), anyString())).thenReturn(success());

        poller.poll();

        verify(kafkaTemplate).send(RESULT_TOPIC, events.get(0).getEndToEndId(), events.get(0).getPayload());
        assertThat(savedEvents())
                .hasSize(2)
                .allSatisfy(event -> {
                    assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.SENT);
                    assertThat(event.getSentAt()).isNotNull();
                });
    }

    @Test
    @DisplayName("Deve marcar como SENT apenas os envios bem-sucedidos; falhas permanecem PENDING")
    void shouldKeepFailedEventsPending() {
        var ok = pendingEvent("002001");
        var failed = pendingEvent("002002");
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class))).thenReturn(List.of(ok, failed));
        when(kafkaTemplate.send(RESULT_TOPIC, ok.getEndToEndId(), ok.getPayload())).thenReturn(success());
        when(kafkaTemplate.send(RESULT_TOPIC, failed.getEndToEndId(), failed.getPayload())).thenReturn(failure());

        poller.poll();

        assertThat(savedEvents()).containsExactly(ok);
        assertThat(failed.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(failed.getSentAt()).isNull();
    }

    @Test
    @DisplayName("Envio que não conclui dentro do timeout deve permanecer PENDING")
    void shouldKeepEventPendingWhenSendTimesOut() {
        var ok = pendingEvent("003001");
        var hanging = pendingEvent("003002");
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class))).thenReturn(List.of(ok, hanging));
        when(kafkaTemplate.send(RESULT_TOPIC, ok.getEndToEndId(), ok.getPayload())).thenReturn(success());
        when(kafkaTemplate.send(RESULT_TOPIC, hanging.getEndToEndId(), hanging.getPayload())).thenReturn(new CompletableFuture<>());

        poller.poll();

        assertThat(savedEvents()).containsExactly(ok);
        assertThat(hanging.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
    }

    @Test
    @DisplayName("Deve drenar o backlog em lotes enquanto o lote vier cheio e sem falhas")
    void shouldDrainBacklogWhileBatchesAreFull() {
        List<OutboxEventDocument> fullBatch = IntStream.range(0, BATCH_SIZE).mapToObj(i -> pendingEvent("00400" + i)).toList();
        List<OutboxEventDocument> lastBatch = List.of(pendingEvent("004100"));
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class)))
                .thenReturn(fullBatch, lastBatch);
        when(kafkaTemplate.send(eq(RESULT_TOPIC), anyString(), anyString())).thenReturn(success());

        poller.poll();

        verify(repository, times(2)).findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class));
        verify(repository, times(2)).saveAll(any());
    }

    @Test
    @DisplayName("Não deve buscar novo lote no mesmo ciclo quando houve falha (evita martelar o broker)")
    void shouldStopDrainingWhenBatchHasFailures() {
        List<OutboxEventDocument> fullBatch = IntStream.range(0, BATCH_SIZE).mapToObj(i -> pendingEvent("00500" + i)).toList();
        when(repository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class))).thenReturn(fullBatch);
        when(kafkaTemplate.send(eq(RESULT_TOPIC), anyString(), anyString())).thenReturn(failure());

        poller.poll();

        verify(repository, times(1)).findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Limit.class));
        verify(repository, never()).saveAll(any());
    }
}

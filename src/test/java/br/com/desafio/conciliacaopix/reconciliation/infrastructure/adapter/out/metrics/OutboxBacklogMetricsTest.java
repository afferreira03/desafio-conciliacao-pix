package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.metrics;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxBacklogMetricsTest {

    private static final Instant NOW = Instant.parse("2026-09-23T10:00:00Z");

    private SpringDataOutboxRepository repository;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() {
        repository = mock(SpringDataOutboxRepository.class);
        registry = new SimpleMeterRegistry();
        new OutboxBacklogMetrics(registry, repository, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private double gauge() {
        return registry.get(OutboxBacklogMetrics.OLDEST_PENDING_AGE).gauge().value();
    }

    @Test
    @DisplayName("Deve expor a idade, em segundos, do evento PENDING mais antigo")
    void shouldExposeAgeOfOldestPendingEvent() {
        when(repository.findFirstByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING))
                .thenReturn(Optional.of(OutboxEventDocument.builder().createdAt(NOW.minusSeconds(90)).build()));

        assertThat(gauge()).isEqualTo(90.0);
    }

    @Test
    @DisplayName("Deve expor 0 quando não há eventos pendentes")
    void shouldExposeZeroWhenNothingPending() {
        when(repository.findFirstByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING)).thenReturn(Optional.empty());

        assertThat(gauge()).isZero();
    }

    @Test
    @DisplayName("Deve expor NaN (sem lançar exceção no scrape) quando a consulta falha")
    void shouldExposeNaNWhenQueryFails() {
        when(repository.findFirstByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING))
                .thenThrow(new IllegalStateException("mongo indisponível"));

        assertThat(gauge()).isNaN();
    }
}
package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.metrics;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataOutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;

/**
 * Idade do evento PENDING mais antigo do outbox — sinal de que os resultados estão atrasando.
 * <p>
 * Calculada a cada leitura da métrica (scrape), independente do relay: se o {@code OutboxEventPoller} parar, a idade
 * continua crescendo e o alerta dispara. Custo: uma consulta coberta pelo índice parcial {status, createdAt}.
 */
@Component
public class OutboxBacklogMetrics {

    public static final String OLDEST_PENDING_AGE = "pix.outbox.oldest.pending.age";

    private static final Logger LOG = LoggerFactory.getLogger(OutboxBacklogMetrics.class);

    private final SpringDataOutboxRepository repository;
    private final Clock clock;

    @Autowired
    public OutboxBacklogMetrics(MeterRegistry meterRegistry, SpringDataOutboxRepository repository) {
        this(meterRegistry, repository, Clock.systemUTC());
    }

    OutboxBacklogMetrics(MeterRegistry meterRegistry, SpringDataOutboxRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
        Gauge.builder(OLDEST_PENDING_AGE, this, OutboxBacklogMetrics::oldestPendingAgeSeconds)
                .description("Idade do evento PENDING mais antigo do outbox (0 = sem pendências)")
                .baseUnit("seconds")
                .strongReference(true) // gauge guarda referência fraca por padrão; evita virar NaN se o objeto for coletado
                .register(meterRegistry);
    }

    double oldestPendingAgeSeconds() {
        try {
            return repository.findFirstByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING)
                    .map(event -> Math.max(0, Duration.between(event.getCreatedAt(), clock.instant()).toMillis() / 1000.0))
                    .orElse(0.0);
        } catch (RuntimeException e) {
            // Um scrape nunca deve falhar por causa do banco: NaN = valor desconhecido.
            LOG.debug("Não foi possível calcular a idade do outbox pendente", e);
            return Double.NaN;
        }
    }
}
package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.metrics;

import br.com.desafio.conciliacaopix.reconciliation.application.port.out.ReconciliationMetricsPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Component
public class MicrometerReconciliationMetricsAdapter implements ReconciliationMetricsPort {

    public static final String RECONCILIATION_COUNTER = "pix.reconciliation.total";
    public static final String LATENCY_TIMER = "pix.reconciliation.latency";
    public static final String DUPLICATE_COUNTER = "pix.reconciliation.duplicates";

    private static final Duration LATENCY_SLO = Duration.ofSeconds(2);

    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final Timer latencyTimer;
    private final Counter duplicateCounter;

    @Autowired
    public MicrometerReconciliationMetricsAdapter(MeterRegistry meterRegistry) {
        this(meterRegistry, Clock.systemUTC());
    }

    MicrometerReconciliationMetricsAdapter(MeterRegistry meterRegistry, Clock clock) {
        this.meterRegistry = meterRegistry;
        this.clock = clock;
        this.latencyTimer = Timer.builder(LATENCY_TIMER)
                .description("Latência ponta a ponta: paymentTimestamp do Pix até a conciliação persistida")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .serviceLevelObjectives(LATENCY_SLO)
                .register(meterRegistry);
        this.duplicateCounter = Counter.builder(DUPLICATE_COUNTER)
                .description("Transações Pix recebidas novamente (endToEndId já conciliado)")
                .register(meterRegistry);
    }

    @Override
    public void recordReconciled(ReconciliationRecord reconciliationRecord, Instant paymentTimestamp) {
        String reason = reconciliationRecord.getInconsistencyReason() != null
                ? reconciliationRecord.getInconsistencyReason().name()
                : "none";

        Counter.builder(RECONCILIATION_COUNTER)
                .description("Conciliações persistidas por status e motivo")
                .tag("status", reconciliationRecord.getStatus().name())
                .tag("reason", reason)
                .register(meterRegistry)
                .increment();

        if (paymentTimestamp != null) {
            Duration latency = Duration.between(paymentTimestamp, clock.instant());
            // Relógios dessincronizados podem gerar latência negativa; não distorce o histograma.
            latencyTimer.record(latency.isNegative() ? Duration.ZERO : latency);
        }
    }

    @Override
    public void recordDuplicate() {
        duplicateCounter.increment();
    }
}
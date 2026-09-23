package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.metrics;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerReconciliationMetricsAdapterTest {

    private static final EndToEndId E2E_ID = EndToEndId.of("E0000000020260919123456789012345");
    private static final Instant NOW = Instant.parse("2026-09-23T10:00:01.500Z");

    private SimpleMeterRegistry registry;
    private MicrometerReconciliationMetricsAdapter adapter;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        adapter = new MicrometerReconciliationMetricsAdapter(registry, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Deve contar a conciliação por status e motivo")
    void shouldCountByStatusAndReason() {
        adapter.recordReconciled(ReconciliationRecord.createReconciled(E2E_ID, TxId.of("TX1"), Money.of(10.00)), NOW);
        adapter.recordReconciled(ReconciliationRecord.createInconsistent(
                E2E_ID, TxId.of("TX1"), Money.of(10.00), Money.of(12.00), InconsistencyReason.AMOUNT_MISMATCH), NOW);

        assertThat(registry.get(MicrometerReconciliationMetricsAdapter.RECONCILIATION_COUNTER)
                .tags("status", "CONCILIADO", "reason", "none").counter().count()).isEqualTo(1.0);
        assertThat(registry.get(MicrometerReconciliationMetricsAdapter.RECONCILIATION_COUNTER)
                .tags("status", "INCONSISTENTE", "reason", "AMOUNT_MISMATCH").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Deve registrar a latência entre o paymentTimestamp e o momento da conciliação")
    void shouldRecordLatencyFromPaymentTimestamp() {
        adapter.recordReconciled(ReconciliationRecord.createPending(E2E_ID, null, Money.of(10.00)),
                Instant.parse("2026-09-23T10:00:00Z"));

        Timer timer = registry.get(MicrometerReconciliationMetricsAdapter.LATENCY_TIMER).timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(1500.0);
    }

    @Test
    @DisplayName("Deve registrar latência zero quando o paymentTimestamp está no futuro (relógio dessincronizado)")
    void shouldClampNegativeLatencyToZero() {
        adapter.recordReconciled(ReconciliationRecord.createPending(E2E_ID, null, Money.of(10.00)), NOW.plusSeconds(5));

        Timer timer = registry.get(MicrometerReconciliationMetricsAdapter.LATENCY_TIMER).timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isZero();
    }

    @Test
    @DisplayName("Deve contar duplicatas separadamente")
    void shouldCountDuplicates() {
        adapter.recordDuplicate();
        adapter.recordDuplicate();

        assertThat(registry.get(MicrometerReconciliationMetricsAdapter.DUPLICATE_COUNTER).counter().count())
                .isEqualTo(2.0);
    }
}
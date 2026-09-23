package br.com.desafio.conciliacaopix.reconciliation.application.service;

import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary.StatusSummary;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationTotals;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.ReconciliationQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReconciliationQueryServiceTest {

    @Mock
    private LoadReconciliationPort loadReconciliationPort;

    @Mock
    private ReconciliationQueryPort reconciliationQueryPort;

    private ReconciliationQueryService service;

    @BeforeEach
    void setUp() {
        service = new ReconciliationQueryService(loadReconciliationPort, reconciliationQueryPort);
    }

    private StatusSummary statusOf(ReconciliationSummary summary, ReconciliationStatus status) {
        return summary.statuses().stream().filter(s -> s.status() == status).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("Relatório deve listar todos os status, inclusive os sem ocorrências")
    void shouldListAllStatusesEvenWithoutOccurrences() {
        when(reconciliationQueryPort.totals(null, null)).thenReturn(List.of(
                new ReconciliationTotals(ReconciliationStatus.CONCILIADO, null, 3, Money.of(300.00))
        ));

        ReconciliationSummary summary = service.summarize(null, null);

        assertThat(summary.statuses()).extracting(StatusSummary::status)
                .containsExactly(ReconciliationStatus.values());
        assertThat(statusOf(summary, ReconciliationStatus.PENDENTE).count()).isZero();
        assertThat(statusOf(summary, ReconciliationStatus.PENDENTE).total()).isEqualTo(Money.ZERO);
    }

    @Test
    @DisplayName("Relatório deve somar totais por status e detalhar INCONSISTENTE por motivo")
    void shouldAggregateTotalsAndBreakDownInconsistenciesByReason() {
        when(reconciliationQueryPort.totals(null, null)).thenReturn(List.of(
                new ReconciliationTotals(ReconciliationStatus.CONCILIADO, null, 10, Money.of(1000.00)),
                new ReconciliationTotals(ReconciliationStatus.PENDENTE, null, 4, Money.of(200.00)),
                new ReconciliationTotals(ReconciliationStatus.INCONSISTENTE, InconsistencyReason.AMOUNT_MISMATCH, 5, Money.of(499.95)),
                new ReconciliationTotals(ReconciliationStatus.INCONSISTENTE, InconsistencyReason.INVOICE_ALREADY_PAID, 2, Money.of(300.00))
        ));

        ReconciliationSummary summary = service.summarize(null, null);

        assertThat(summary.totalCount()).isEqualTo(21);
        assertThat(summary.totalAmount()).isEqualTo(Money.of(1999.95));

        StatusSummary inconsistent = statusOf(summary, ReconciliationStatus.INCONSISTENTE);
        assertThat(inconsistent.count()).isEqualTo(7);
        assertThat(inconsistent.total()).isEqualTo(Money.of(799.95));
        assertThat(inconsistent.reasons())
                .extracting(ReconciliationSummary.ReasonSummary::reason)
                .containsExactly(InconsistencyReason.AMOUNT_MISMATCH, InconsistencyReason.INVOICE_ALREADY_PAID);

        assertThat(statusOf(summary, ReconciliationStatus.CONCILIADO).reasons()).isEmpty();
    }

    @Test
    @DisplayName("Relatório deve rejeitar período com início posterior ao fim")
    void shouldRejectInvertedPeriod() {
        Instant now = Instant.now();

        assertThatThrownBy(() -> service.summarize(now, now.minus(1, ChronoUnit.HOURS)))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(reconciliationQueryPort);
    }
}

package br.com.desafio.conciliacaopix.reconciliation.application.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApplicationModelTest {

    @Test
    @DisplayName("PageQuery deve rejeitar página negativa e tamanho fora de 1..100")
    void pageQueryShouldValidateBounds() {
        assertThatThrownBy(() -> new PageQuery(-1, 20)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PageQuery(0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PageQuery(0, 101)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new PageQuery(2, 20).offset()).isEqualTo(40);
    }

    @Test
    @DisplayName("PageResult deve calcular o total de páginas")
    void pageResultShouldComputeTotalPages() {
        assertThat(new PageResult<>(List.of(), 0, 20, 41).totalPages()).isEqualTo(3);
        assertThat(new PageResult<>(List.of(), 0, 20, 0).totalPages()).isZero();
    }

    @Test
    @DisplayName("Critério de busca deve rejeitar período invertido")
    void searchCriteriaShouldRejectInvertedPeriod() {
        Instant now = Instant.now();

        assertThatThrownBy(() -> new ReconciliationSearchCriteria(null, null, now, now.minus(1, ChronoUnit.DAYS)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new ReconciliationSearchCriteria(null, null, null, null)).isNotNull();
    }
}

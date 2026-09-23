package br.com.desafio.conciliacaopix.reconciliation.domain.model.vo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueObjectsTest {

    @Nested
    @DisplayName("Money")
    class MoneyTest {

        @Test
        @DisplayName("Deve rejeitar valor negativo e nulo")
        void shouldRejectNegativeAndNull() {
            assertThatThrownBy(() -> Money.of(-0.01)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Money.of((BigDecimal) null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("Deve normalizar para 2 casas decimais (HALF_EVEN)")
        void shouldNormalizeScale() {
            assertThat(Money.of("150").value()).isEqualByComparingTo("150.00");
            assertThat(Money.of("150").value().scale()).isEqualTo(2);
            assertThat(Money.of("10.005").value()).isEqualByComparingTo("10.00");
            assertThat(Money.of("10.015").value()).isEqualByComparingTo("10.02");
        }

        @Test
        @DisplayName("Igualdade de valor independe da escala de entrada")
        void equalityShouldIgnoreInputScale() {
            assertThat(Money.of("150.0")).isEqualTo(Money.of(150.00));
            assertThat(Money.of("150.0").isEqualTo(Money.of("150.00"))).isTrue();
            assertThat(Money.of("149.99").isLessThan(Money.of("150.00"))).isTrue();
            assertThat(Money.of("100.10").plus(Money.of("0.90"))).isEqualTo(Money.of("101.00"));
        }
    }

    @Nested
    @DisplayName("EndToEndId")
    class EndToEndIdTest {

        @Test
        @DisplayName("Deve aceitar 32 caracteres alfanuméricos iniciando com 'E'")
        void shouldAcceptValidFormat() {
            assertThat(EndToEndId.of("E0000000020260919123456789012345").value()).hasSize(32);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {
                "X0000000020260919123456789012345",  // não inicia com E
                "E000000002026091912345678901234",   // 31 caracteres
                "E00000000202609191234567890123456", // 33 caracteres
                "E000000002026091912345678901234-"   // caractere inválido
        })
        @DisplayName("Deve rejeitar formatos inválidos")
        void shouldRejectInvalidFormats(String value) {
            assertThatThrownBy(() -> EndToEndId.of(value)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("TxId")
    class TxIdTest {

        @ParameterizedTest
        @ValueSource(strings = {"T", "TX123", "abcdefghijABCDEFGHIJ012345678901234"})
        @DisplayName("Deve aceitar de 1 a 35 caracteres alfanuméricos")
        void shouldAcceptValidFormats(String value) {
            assertThat(TxId.of(value).value()).isEqualTo(value);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {" ", "TX-123", "abcdefghijABCDEFGHIJ0123456789012345"})
        @DisplayName("Deve rejeitar vazio, caracteres especiais e mais de 35 caracteres")
        void shouldRejectInvalidFormats(String value) {
            assertThatThrownBy(() -> TxId.of(value)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}

package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class PixKeyMaskerTest {

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "user@email.com, use***@email.com",
            "ab@email.com, ab***@email.com",
            "12345678901, *******8901",
            "+5511987654321, **********4321",
            "abcdefgh-1234, *********1234",
            "abc, ***"
    })
    void shouldMaskPixKey(String pixKey, String expected) {
        assertThat(PixKeyMasker.mask(pixKey)).isEqualTo(expected);
    }
}

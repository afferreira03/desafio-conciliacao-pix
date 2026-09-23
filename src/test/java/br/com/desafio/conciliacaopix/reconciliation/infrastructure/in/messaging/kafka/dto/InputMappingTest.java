package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka.dto;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconciliationPixCommand;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.PixTransaction;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mapeamento da entrada Kafka (DTO) → comando → domínio.
 */
class InputMappingTest {

    private static final String E2E = "E0000000020260919123456789012345";

    @Test
    @DisplayName("DTO deve mapear campos e converter valor para Money")
    void dtoShouldMapToCommand() {
        Instant paidAt = Instant.parse("2026-09-23T10:00:00Z");
        var dto = new PixTransactionEventDto(E2E, "TX123", new BigDecimal("150.5"), paidAt, "user@email.com");

        ReconciliationPixCommand command = dto.toCommand();

        assertThat(command.endToEndId()).isEqualTo(E2E);
        assertThat(command.txId()).isEqualTo("TX123");
        assertThat(command.amount()).isEqualTo(Money.of("150.50"));
        assertThat(command.paymentTimestamp()).isEqualTo(paidAt);
        assertThat(command.pixKey()).isEqualTo("user@email.com");
    }

    @Test
    @DisplayName("DTO sem paymentTimestamp deve assumir o instante de recebimento")
    void dtoWithoutTimestampShouldDefaultToNow() {
        var dto = new PixTransactionEventDto(E2E, "TX123", new BigDecimal("150.00"), null, null);

        Instant timestamp = dto.toCommand().paymentTimestamp();

        assertThat(timestamp).isCloseTo(Instant.now(), org.assertj.core.api.Assertions.within(5, ChronoUnit.SECONDS));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    @DisplayName("txId nulo ou em branco deve virar ausência de txId (aciona o fallback por chave Pix)")
    void blankTxIdShouldBecomeAbsent(String txId) {
        var command = new ReconciliationPixCommand(E2E, txId, Money.of(150.00), Instant.now(), "user@email.com");

        PixTransaction transaction = command.toDomain();

        assertThat(transaction.getTxId()).isEmpty();
    }

    @Test
    @DisplayName("txId preenchido deve ser mapeado para o value object")
    void presentTxIdShouldBeMapped() {
        var command = new ReconciliationPixCommand(E2E, "TX123", Money.of(150.00), Instant.now(), null);

        assertThat(command.toDomain().getTxId()).contains(TxId.of("TX123"));
    }
}

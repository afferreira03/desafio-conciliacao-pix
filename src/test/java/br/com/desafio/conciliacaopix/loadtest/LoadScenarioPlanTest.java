package br.com.desafio.conciliacaopix.loadtest;

import br.com.desafio.conciliacaopix.loadtest.LoadScenarioPlan.PixMessage;
import br.com.desafio.conciliacaopix.loadtest.LoadScenarioPlan.Scenario;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class LoadScenarioPlanTest {

    private final LoadScenarioPlan plan = LoadScenarioPlan.create(5_000);

    @Test
    @DisplayName("Deve gerar o mix de 5.000 mensagens com o resultado esperado do desenho da demo")
    void shouldBuildExpectedMix() {
        var expected = plan.expected();

        assertThat(expected.messages()).isEqualTo(5_000);
        assertThat(expected.records()).isEqualTo(4_750);
        assertThat(expected.conciliado()).isEqualTo(3_500);
        assertThat(expected.inconsistente()).isEqualTo(750);
        assertThat(expected.amountMismatch()).isEqualTo(500);
        assertThat(expected.alreadyPaid()).isEqualTo(250);
        assertThat(expected.pendente()).isEqualTo(500);
        assertThat(plan.invoices()).hasSize(4_000);
        assertThat(plan.phaseOne().size() + plan.phaseTwo().size()).isEqualTo(5_000);
    }

    @Test
    @DisplayName("Deve gerar endToEndId e txId válidos para os value objects do domínio")
    void shouldGenerateValidIdentifiers() {
        all().forEach(m -> {
            assertThatCode(() -> EndToEndId.of(m.endToEndId())).doesNotThrowAnyException();
            if (m.txId() != null) {
                assertThatCode(() -> TxId.of(m.txId())).doesNotThrowAnyException();
            }
            assertThat(m.amount()).isGreaterThan(BigDecimal.ZERO);
        });
        plan.invoices().forEach(i -> assertThatCode(() -> TxId.of(i.txId())).doesNotThrowAnyException());
    }

    @Test
    @DisplayName("Somente reenvios repetem endToEndId; fase 2 só contém segundo pagamento e reenvio")
    void shouldOnlyRepeatEndToEndIdOnRedelivery() {
        Set<String> seen = new HashSet<>();
        plan.phaseOne().forEach(m -> assertThat(seen.add(m.endToEndId())).isTrue());
        plan.phaseTwo().forEach(m -> {
            if (m.scenario() == Scenario.REDELIVERY) {
                assertThat(seen).contains(m.endToEndId());
            } else {
                assertThat(m.scenario()).isEqualTo(Scenario.SECOND_PAYMENT);
                assertThat(seen.add(m.endToEndId())).isTrue();
            }
        });
    }

    @Test
    @DisplayName("Fallback não envia txId e usa o endToEndId como chave de partição")
    void fallbackMessagesHaveNoTxId() {
        List<PixMessage> fallbacks = all().filter(m -> m.scenario() == Scenario.FALLBACK).toList();

        assertThat(fallbacks).hasSize(250).allSatisfy(m -> {
            assertThat(m.txId()).isNull();
            assertThat(m.key()).isEqualTo(m.endToEndId());
        });
    }

    @Test
    @DisplayName("Deve serializar no formato do PixTransactionEventDto")
    void shouldSerializeLikeDto() {
        PixMessage fallback = all().filter(m -> m.scenario() == Scenario.FALLBACK).findFirst().orElseThrow();

        String json = PixLoadGenerator.toJson(fallback, Instant.parse("2026-09-23T10:00:00Z"));

        assertThat(json).contains("\"txId\":null", "\"transactionAmount\":" + fallback.amount().toPlainString(),
                "\"paymentTimestamp\":\"2026-09-23T10:00:00Z\"", "\"endToEndId\":\"" + fallback.endToEndId() + "\"");
    }

    private Stream<PixMessage> all() {
        return Stream.concat(plan.phaseOne().stream(), plan.phaseTwo().stream());
    }
}
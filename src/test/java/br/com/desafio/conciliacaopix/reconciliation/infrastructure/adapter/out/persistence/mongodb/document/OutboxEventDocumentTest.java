package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document;

import br.com.desafio.conciliacaopix.reconciliation.domain.event.DomainEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixInconsistentEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixPendingEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixReconciledEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventDocumentTest {

    private static final EndToEndId E2E = EndToEndId.of("E0000000020260919123456789012345");
    private static final TxId TX = TxId.of("TX123");

    static Stream<Arguments> events() {
        return Stream.of(
                Arguments.of(PixReconciledEvent.of("R1", E2E, TX, Money.of(150.00)), "PixReconciledEvent"),
                Arguments.of(PixPendingEvent.of("R2", E2E, TX, Money.of(150.00)), "PixPendingEvent"),
                Arguments.of(PixInconsistentEvent.of("R3", E2E, TX, Money.of(149.99), Money.of(150.00),
                        InconsistencyReason.AMOUNT_MISMATCH), "PixInconsistentEvent")
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("events")
    void shouldBuildPendingOutboxRowForEachEventType(DomainEvent event, String expectedType) {
        OutboxEventDocument document = OutboxEventDocument.fromDomain(event, "{\"json\":true}");

        assertThat(document.getId()).isNotBlank();
        assertThat(document.getEventType()).isEqualTo(expectedType);
        assertThat(document.getEndToEndId()).isEqualTo(E2E.value());
        assertThat(document.getPayload()).isEqualTo("{\"json\":true}");
        assertThat(document.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(document.getCreatedAt()).isNotNull();
        assertThat(document.getSentAt()).isNull();
    }
}

package br.com.desafio.conciliacaopix.reconciliation.domain.model;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InvoiceTest {

    private static final Instant NOW = Instant.now();

    private Invoice createInvoice(InvoiceStatus status, Instant expiration) {
        return new Invoice(
                "INV-1",
                TxId.of("TX123"),
                Money.of(150.00),
                status,
                NOW.minus(1, ChronoUnit.HOURS),
                expiration
        );
    }

    @Test
    @DisplayName("Deve abrir nova fatura com status ABERTA, id gerado e data de criação")
    void shouldOpenNewInvoice() {
        var expiration = Instant.now().plus(1, ChronoUnit.DAYS);

        var invoice = Invoice.open(TxId.of("TX123"), Money.of(150.00), expiration);

        assertThat(invoice.getStatus()).isEqualTo(InvoiceStatus.ABERTA);
        assertThat(invoice.getId()).isNotBlank();
        assertThat(invoice.getCreatedAt()).isBefore(expiration);
        assertThat(invoice.getExpirationDate()).isEqualTo(expiration);
    }

    @Test
    @DisplayName("Deve rejeitar abertura de fatura com valor zero ou expiração no passado")
    void shouldRejectInvalidInvoiceOpening() {
        assertThatThrownBy(() -> Invoice.open(TxId.of("TX123"), Money.ZERO, Instant.now().plus(1, ChronoUnit.DAYS)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Invoice.open(TxId.of("TX123"), Money.of(150.00), Instant.now().minus(1, ChronoUnit.MINUTES)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Deve marcar fatura ABERTA como PAGA")
    void shouldMarkOpenInvoiceAsPaid() {
        var invoice = createInvoice(InvoiceStatus.ABERTA, NOW.plus(1, ChronoUnit.DAYS));

        invoice.markAsPaid();

        assertThat(invoice.getStatus()).isEqualTo(InvoiceStatus.PAGA);
    }

    @ParameterizedTest
    @EnumSource(value = InvoiceStatus.class, names = "ABERTA", mode = EnumSource.Mode.EXCLUDE)
    @DisplayName("Deve rejeitar pagamento de fatura que não está ABERTA")
    void shouldRejectPaymentWhenInvoiceIsNotOpen(InvoiceStatus status) {
        var invoice = createInvoice(status, NOW.plus(1, ChronoUnit.DAYS));

        assertThatThrownBy(invoice::markAsPaid)
                .isInstanceOf(IllegalStateException.class);
        assertThat(invoice.getStatus()).isEqualTo(status);
    }

    @ParameterizedTest
    @EnumSource(InvoiceStatus.class)
    @DisplayName("isOpen deve ser verdadeiro somente para ABERTA")
    void shouldBeOpenOnlyWhenStatusIsAberta(InvoiceStatus status) {
        var invoice = createInvoice(status, NOW.plus(1, ChronoUnit.DAYS));

        assertThat(invoice.isOpen()).isEqualTo(status == InvoiceStatus.ABERTA);
    }

    @Test
    @DisplayName("Deve marcar como EXPIRADA quando a referência é posterior à data de expiração")
    void shouldExpireWhenReferenceIsAfterExpiration() {
        var invoice = createInvoice(InvoiceStatus.ABERTA, NOW.minus(1, ChronoUnit.MINUTES));

        invoice.markAsExpired(NOW);

        assertThat(invoice.getStatus()).isEqualTo(InvoiceStatus.EXPIRADA);
    }

    @Test
    @DisplayName("Não deve expirar quando a referência é anterior à data de expiração")
    void shouldNotExpireWhenReferenceIsBeforeExpiration() {
        var invoice = createInvoice(InvoiceStatus.ABERTA, NOW.plus(1, ChronoUnit.DAYS));

        invoice.markAsExpired(NOW);

        assertThat(invoice.getStatus()).isEqualTo(InvoiceStatus.ABERTA);
    }
}

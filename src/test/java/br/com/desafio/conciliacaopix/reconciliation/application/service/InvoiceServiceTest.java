package br.com.desafio.conciliacaopix.reconciliation.application.service;

import br.com.desafio.conciliacaopix.reconciliation.application.exception.InvoiceAlreadyExistsException;
import br.com.desafio.conciliacaopix.reconciliation.application.model.InvoiceView;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.CreateInvoiceCommand;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.InvoiceQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class InvoiceServiceTest {

    @Mock
    private SaveInvoicePort saveInvoicePort;

    @Mock
    private InvoiceQueryPort invoiceQueryPort;

    private InvoiceService service;

    @BeforeEach
    void setUp() {
        service = new InvoiceService(saveInvoicePort, invoiceQueryPort);
    }

    private CreateInvoiceCommand command() {
        return new CreateInvoiceCommand("TX123", Money.of(150.00), "user@email.com", Instant.now().plus(1, ChronoUnit.DAYS));
    }

    @Test
    @DisplayName("Deve abrir fatura ABERTA e persistir com a chave Pix")
    void shouldOpenInvoiceAndPersistWithPixKey() {
        InvoiceView view = service.create(command());

        ArgumentCaptor<Invoice> captor = ArgumentCaptor.forClass(Invoice.class);
        verify(saveInvoicePort).save(captor.capture(), eq("user@email.com"));

        Invoice saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(InvoiceStatus.ABERTA);
        assertThat(saved.getTxId()).isEqualTo(TxId.of("TX123"));
        assertThat(saved.getId()).isNotBlank();

        assertThat(view.status()).isEqualTo(InvoiceStatus.ABERTA);
        assertThat(view.pixKey()).isEqualTo("user@email.com");
        assertThat(view.amount()).isEqualTo(Money.of(150.00));
    }

    @Test
    @DisplayName("Deve propagar conflito quando o txId já existe")
    void shouldPropagateConflictWhenTxIdAlreadyExists() {
        doThrow(new InvoiceAlreadyExistsException("TX123")).when(saveInvoicePort).save(any(), any());

        assertThatThrownBy(() -> service.create(command()))
                .isInstanceOf(InvoiceAlreadyExistsException.class);
    }

    @Test
    @DisplayName("Deve rejeitar txId com formato inválido antes de persistir")
    void shouldRejectInvalidTxId() {
        var invalid = new CreateInvoiceCommand("TX-123!", Money.of(150.00), "user@email.com", Instant.now().plus(1, ChronoUnit.DAYS));

        assertThatThrownBy(() -> service.create(invalid)).isInstanceOf(IllegalArgumentException.class);
    }
}

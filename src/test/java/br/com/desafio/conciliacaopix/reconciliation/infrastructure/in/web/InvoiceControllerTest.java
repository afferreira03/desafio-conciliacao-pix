package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web;

import br.com.desafio.conciliacaopix.reconciliation.application.exception.InvoiceAlreadyExistsException;
import br.com.desafio.conciliacaopix.reconciliation.application.model.InvoiceView;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ManageInvoiceUseCase;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.error.ApiExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.hamcrest.Matchers.endsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class InvoiceControllerTest {

    private static final String PROBLEM_JSON = "application/problem+json";

    private ManageInvoiceUseCase useCase;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        useCase = mock(ManageInvoiceUseCase.class);

        var validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        mockMvc = MockMvcBuilders.standaloneSetup(new InvoiceController(useCase))
                .setControllerAdvice(new ApiExceptionHandler())
                .setValidator(validator)
                .build();
    }

    private InvoiceView openInvoiceView() {
        Instant now = Instant.now();
        return new InvoiceView(TxId.of("TX123"), Money.of(150.00), InvoiceStatus.ABERTA, "user@email.com",
                now, now.plus(1, ChronoUnit.DAYS));
    }

    private String body(String txId, String amount, String expiresAt) {
        return """
                {"txId": "%s", "amount": %s, "pixKey": "user@email.com", "expiresAt": "%s"}
                """.formatted(txId, amount, expiresAt);
    }

    private String future() {
        return Instant.now().plus(1, ChronoUnit.DAYS).toString();
    }

    @Test
    @DisplayName("POST deve criar fatura, retornar 201 com Location e chave Pix mascarada")
    void shouldCreateInvoice() throws Exception {
        when(useCase.create(any())).thenReturn(openInvoiceView());

        mockMvc.perform(post("/api/v1/invoices")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("TX123", "150.00", future())))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", endsWith("/api/v1/invoices/TX123")))
                .andExpect(jsonPath("$.status").value("ABERTA"))
                .andExpect(jsonPath("$.pixKey").value("use***@email.com"));
    }

    @Test
    @DisplayName("POST deve retornar 400 para valor negativo e txId inválido")
    void shouldReturn400ForInvalidBody() throws Exception {
        mockMvc.perform(post("/api/v1/invoices")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("TX-123!", "-10.00", future())))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(PROBLEM_JSON));

        verifyNoInteractions(useCase);
    }

    @Test
    @DisplayName("POST deve retornar 400 para expiração no passado")
    void shouldReturn400ForPastExpiration() throws Exception {
        mockMvc.perform(post("/api/v1/invoices")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("TX123", "150.00", Instant.now().minus(1, ChronoUnit.DAYS).toString())))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(useCase);
    }

    @Test
    @DisplayName("POST deve retornar 409 quando o txId já existe")
    void shouldReturn409WhenTxIdAlreadyExists() throws Exception {
        when(useCase.create(any())).thenThrow(new InvoiceAlreadyExistsException("TX123"));

        mockMvc.perform(post("/api/v1/invoices")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("TX123", "150.00", future())))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(409));
    }

    @Test
    @DisplayName("GET /{txId} deve retornar 200 com chave mascarada")
    void shouldReturnInvoiceByTxId() throws Exception {
        when(useCase.findByTxId(TxId.of("TX123"))).thenReturn(Optional.of(openInvoiceView()));

        mockMvc.perform(get("/api/v1/invoices/{txId}", "TX123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.txId").value("TX123"))
                .andExpect(jsonPath("$.pixKey").value("use***@email.com"));
    }

    @Test
    @DisplayName("GET /{txId} deve retornar 404 quando a fatura não existe")
    void shouldReturn404WhenInvoiceNotFound() throws Exception {
        when(useCase.findByTxId(any())).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/invoices/{txId}", "TX999"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(PROBLEM_JSON));
    }
}

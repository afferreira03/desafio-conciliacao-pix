package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web;

import br.com.desafio.conciliacaopix.reconciliation.application.model.PageQuery;
import br.com.desafio.conciliacaopix.reconciliation.application.model.PageResult;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSearchCriteria;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.QueryReconciliationUseCase;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.*;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.error.ApiExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ReconciliationControllerTest {

    private static final String E2E = "E0000000020260919123456789012345";
    private static final String PROBLEM_JSON = "application/problem+json";

    private QueryReconciliationUseCase useCase;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        useCase = mock(QueryReconciliationUseCase.class);

        var validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        mockMvc = MockMvcBuilders.standaloneSetup(new ReconciliationController(useCase))
                .setControllerAdvice(new ApiExceptionHandler())
                .setValidator(validator)
                .build();
    }

    private ReconciliationRecord inconsistentRecord() {
        return ReconciliationRecord.createInconsistent(
                EndToEndId.of(E2E), TxId.of("TX123"), Money.of(149.99), Money.of(150.00), InconsistencyReason.AMOUNT_MISMATCH);
    }

    @Test
    @DisplayName("GET /{endToEndId} deve retornar 200 com a conciliação")
    void shouldReturnReconciliationByEndToEndId() throws Exception {
        when(useCase.findByEndToEndId(EndToEndId.of(E2E))).thenReturn(Optional.of(inconsistentRecord()));

        mockMvc.perform(get("/api/v1/reconciliations/{e2e}", E2E))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.endToEndId").value(E2E))
                .andExpect(jsonPath("$.status").value("INCONSISTENTE"))
                .andExpect(jsonPath("$.inconsistencyReason").value("AMOUNT_MISMATCH"))
                .andExpect(jsonPath("$.transactionAmount").value(149.99))
                .andExpect(jsonPath("$.expectedAmount").value(150.00));
    }

    @Test
    @DisplayName("GET /{endToEndId} deve retornar 404 ProblemDetail quando não existe")
    void shouldReturn404WhenNotFound() throws Exception {
        when(useCase.findByEndToEndId(any())).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/reconciliations/{e2e}", E2E))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.title").value("Recurso não encontrado"));
    }

    @Test
    @DisplayName("GET /{endToEndId} deve retornar 400 para endToEndId malformado")
    void shouldReturn400ForMalformedEndToEndId() throws Exception {
        mockMvc.perform(get("/api/v1/reconciliations/{e2e}", "invalido"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(PROBLEM_JSON));

        verifyNoInteractions(useCase);
    }

    @Test
    @DisplayName("GET lista deve repassar filtros e paginação e retornar a página")
    void shouldSearchWithFiltersAndPagination() throws Exception {
        when(useCase.search(any(), any())).thenReturn(new PageResult<>(List.of(inconsistentRecord()), 1, 10, 11));

        mockMvc.perform(get("/api/v1/reconciliations")
                        .param("status", "INCONSISTENTE")
                        .param("reason", "AMOUNT_MISMATCH")
                        .param("from", "2026-09-23T00:00:00Z")
                        .param("page", "1")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].endToEndId").value(E2E))
                .andExpect(jsonPath("$.totalElements").value(11))
                .andExpect(jsonPath("$.totalPages").value(2));

        ArgumentCaptor<ReconciliationSearchCriteria> criteria = ArgumentCaptor.forClass(ReconciliationSearchCriteria.class);
        ArgumentCaptor<PageQuery> page = ArgumentCaptor.forClass(PageQuery.class);
        verify(useCase).search(criteria.capture(), page.capture());

        assertThat(criteria.getValue().status()).isEqualTo(ReconciliationStatus.INCONSISTENTE);
        assertThat(criteria.getValue().reason()).isEqualTo(InconsistencyReason.AMOUNT_MISMATCH);
        assertThat(criteria.getValue().from()).isNotNull();
        assertThat(criteria.getValue().to()).isNull();
        assertThat(page.getValue()).isEqualTo(new PageQuery(1, 10));
    }

    @Test
    @DisplayName("GET lista deve retornar 400 quando size excede o máximo")
    void shouldReturn400WhenPageSizeTooLarge() throws Exception {
        mockMvc.perform(get("/api/v1/reconciliations").param("size", "500"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Requisição inválida"))
                .andExpect(jsonPath("$.errors.length()").value(1))
                .andExpect(jsonPath("$.errors[0].field").value("size"))
                .andExpect(jsonPath("$.errors[0].message").isNotEmpty());

        verifyNoInteractions(useCase);
    }

    @Test
    @DisplayName("GET lista deve retornar 400 para status inexistente")
    void shouldReturn400ForUnknownStatus() throws Exception {
        mockMvc.perform(get("/api/v1/reconciliations").param("status", "XYZ"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(useCase);
    }

    @Test
    @DisplayName("GET /summary deve retornar o relatório por status")
    void shouldReturnSummary() throws Exception {
        var summary = new ReconciliationSummary(null, null, 7, Money.of(799.95), List.of(
                new ReconciliationSummary.StatusSummary(ReconciliationStatus.CONCILIADO, 0, Money.ZERO, List.of()),
                new ReconciliationSummary.StatusSummary(ReconciliationStatus.PENDENTE, 0, Money.ZERO, List.of()),
                new ReconciliationSummary.StatusSummary(ReconciliationStatus.INCONSISTENTE, 7, Money.of(799.95), List.of(
                        new ReconciliationSummary.ReasonSummary(InconsistencyReason.AMOUNT_MISMATCH, 7, Money.of(799.95))
                ))
        ));
        when(useCase.summarize(null, null)).thenReturn(summary);

        mockMvc.perform(get("/api/v1/reconciliations/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCount").value(7))
                .andExpect(jsonPath("$.statuses.length()").value(3))
                .andExpect(jsonPath("$.statuses[2].status").value("INCONSISTENTE"))
                .andExpect(jsonPath("$.statuses[2].reasons[0].reason").value("AMOUNT_MISMATCH"));
    }
}

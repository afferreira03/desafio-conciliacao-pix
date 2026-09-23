package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web;

import br.com.desafio.conciliacaopix.reconciliation.application.model.PageQuery;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSearchCriteria;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.QueryReconciliationUseCase;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto.PageResponse;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto.ReconciliationResponse;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto.ReconciliationSummaryResponse;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.error.ResourceNotFoundException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/api/v1/reconciliations")
@Tag(name = "Conciliações", description = "Consulta e relatórios de conciliação de transações Pix.")
public class ReconciliationController {

    private final QueryReconciliationUseCase queryReconciliationUseCase;

    public ReconciliationController(QueryReconciliationUseCase queryReconciliationUseCase) {
        this.queryReconciliationUseCase = queryReconciliationUseCase;
    }

    @GetMapping("/{endToEndId}")
    @Operation(summary = "Consulta a conciliação de uma transação Pix pelo endToEndId")
    @ApiResponse(responseCode = "200", description = "Conciliação encontrada")
    @ApiResponse(responseCode = "400", description = "endToEndId com formato inválido",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "404", description = "Nenhuma conciliação para o endToEndId",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    public ReconciliationResponse findByEndToEndId(
            @Parameter(description = "Identificador fim-a-fim da transação Pix (32 caracteres, inicia com 'E').",
                    example = "E0000000020260919123456789012345")
            @PathVariable String endToEndId) {

        return queryReconciliationUseCase.findByEndToEndId(EndToEndId.of(endToEndId))
                .map(ReconciliationResponse::from)
                .orElseThrow(() -> new ResourceNotFoundException("Conciliação", endToEndId));
    }

    @GetMapping
    @Operation(summary = "Lista conciliações com filtros e paginação",
            description = "Ordenado por data de criação (mais recentes primeiro). Use status=INCONSISTENTE e reason "
                    + "para reportar divergências (ex.: reason=AMOUNT_MISMATCH).")
    @ApiResponse(responseCode = "200", description = "Página de conciliações")
    @ApiResponse(responseCode = "400", description = "Filtro ou paginação inválidos",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    public PageResponse<ReconciliationResponse> search(
            @Parameter(description = "Status da conciliação") @RequestParam(required = false) ReconciliationStatus status,
            @Parameter(description = "Motivo da inconsistência") @RequestParam(required = false) InconsistencyReason reason,
            @Parameter(description = "Início do período (ISO-8601, UTC)", example = "2026-09-23T00:00:00Z")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "Fim do período (ISO-8601, UTC)", example = "2026-09-23T23:59:59Z")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @Parameter(description = "Página (base 0)") @RequestParam(defaultValue = "0") @Min(0) int page,
            @Parameter(description = "Itens por página (máx. 100)") @RequestParam(defaultValue = "20") @Min(1) @Max(PageQuery.MAX_SIZE) int size) {

        var criteria = new ReconciliationSearchCriteria(status, reason, from, to);
        var result = queryReconciliationUseCase.search(criteria, new PageQuery(page, size));
        return PageResponse.from(result, ReconciliationResponse::from);
    }

    @GetMapping("/summary")
    @Operation(summary = "Relatório de conciliação por status",
            description = "Quantidade e valor total por status (CONCILIADO, PENDENTE, INCONSISTENTE) e, para "
                    + "INCONSISTENTE, a quebra por motivo. Sem período informado, considera todo o histórico.")
    @ApiResponse(responseCode = "200", description = "Relatório gerado")
    @ApiResponse(responseCode = "400", description = "Período inválido",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    public ReconciliationSummaryResponse summary(
            @Parameter(description = "Início do período (ISO-8601, UTC)", example = "2026-09-23T00:00:00Z")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "Fim do período (ISO-8601, UTC)", example = "2026-09-23T23:59:59Z")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {

        return ReconciliationSummaryResponse.from(queryReconciliationUseCase.summarize(from, to));
    }
}

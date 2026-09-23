package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ManageInvoiceUseCase;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto.CreateInvoiceRequest;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.dto.InvoiceResponse;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.error.ResourceNotFoundException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;

@RestController
@RequestMapping("/api/v1/invoices")
@Tag(name = "Faturas", description = "Cadastro e consulta de faturas (cobranças Pix) usadas na conciliação.")
public class InvoiceController {

    private final ManageInvoiceUseCase manageInvoiceUseCase;

    public InvoiceController(ManageInvoiceUseCase manageInvoiceUseCase) {
        this.manageInvoiceUseCase = manageInvoiceUseCase;
    }

    @PostMapping
    @Operation(summary = "Abre uma nova fatura", description = "A fatura é criada com status ABERTA.")
    @ApiResponse(responseCode = "201", description = "Fatura criada")
    @ApiResponse(responseCode = "400", description = "Dados inválidos",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "409", description = "Já existe fatura com o mesmo txId",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    public ResponseEntity<InvoiceResponse> create(@Valid @RequestBody CreateInvoiceRequest request) {
        InvoiceResponse response = InvoiceResponse.from(manageInvoiceUseCase.create(request.toCommand()));

        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{txId}")
                .buildAndExpand(response.txId())
                .toUri();

        return ResponseEntity.created(location).body(response);
    }

    @GetMapping("/{txId}")
    @Operation(summary = "Consulta uma fatura pelo txId", description = "A chave Pix é retornada mascarada (LGPD).")
    @ApiResponse(responseCode = "200", description = "Fatura encontrada")
    @ApiResponse(responseCode = "400", description = "txId com formato inválido",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "404", description = "Fatura não encontrada",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    public InvoiceResponse findByTxId(
            @Parameter(description = "Identificador da cobrança Pix", example = "TX123") @PathVariable String txId) {

        return manageInvoiceUseCase.findByTxId(TxId.of(txId))
                .map(InvoiceResponse::from)
                .orElseThrow(() -> new ResourceNotFoundException("Fatura", txId));
    }
}

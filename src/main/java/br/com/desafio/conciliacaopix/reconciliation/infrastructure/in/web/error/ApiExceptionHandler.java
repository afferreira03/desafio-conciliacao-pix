package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.error;

import br.com.desafio.conciliacaopix.reconciliation.application.exception.InvoiceAlreadyExistsException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Traduz exceções em respostas ProblemDetail (RFC 9457).
 * <p>
 * Estender {@link ResponseEntityExceptionHandler} faz com que erros do próprio Spring MVC
 * (enum inválido em query param, corpo malformado, {@code HandlerMethodValidationException},
 * {@code MethodArgumentNotValidException}, {@code ErrorResponseException}) também saiam como ProblemDetail.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    /**
     * Violações de invariantes dos value objects / modelos (ex.: EndToEndId malformado, período inválido).
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Requisição inválida");
        return problem;
    }

    @ExceptionHandler(InvoiceAlreadyExistsException.class)
    public ProblemDetail handleInvoiceAlreadyExists(InvoiceAlreadyExistsException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        problem.setTitle("Fatura já existente");
        return problem;
    }
}

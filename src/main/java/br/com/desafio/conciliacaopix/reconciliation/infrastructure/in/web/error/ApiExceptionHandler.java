package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.error;

import br.com.desafio.conciliacaopix.reconciliation.application.exception.InvoiceAlreadyExistsException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Traduz exceções em respostas ProblemDetail (RFC 9457).
 * <p>
 * Estender {@link ResponseEntityExceptionHandler} faz com que erros do próprio Spring MVC
 * (enum inválido em query param, corpo malformado, {@code HandlerMethodValidationException},
 * {@code MethodArgumentNotValidException}, {@code ErrorResponseException}) também saiam como ProblemDetail.
 * Erros de validação ganham a propriedade {@code errors} com o campo e a mensagem de cada violação.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    static final String VALIDATION_TITLE = "Requisição inválida";

    /**
     * Uma violação de validação. O valor rejeitado não é devolvido de propósito: pode conter dado pessoal
     * (ex.: chave Pix).
     */
    public record FieldViolation(String field, String message) {
    }

    /**
     * Violações de invariantes dos value objects / modelos (ex.: EndToEndId malformado, período inválido).
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle(VALIDATION_TITLE);
        return problem;
    }

    @ExceptionHandler(InvoiceAlreadyExistsException.class)
    public ProblemDetail handleInvoiceAlreadyExists(InvoiceAlreadyExistsException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        problem.setTitle("Fatura já existente");
        return problem;
    }

    /**
     * Corpo da requisição inválido ({@code @Valid @RequestBody}).
     */
    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        Stream<FieldViolation> fieldErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> new FieldViolation(error.getField(), message(error)));
        Stream<FieldViolation> globalErrors = ex.getBindingResult().getGlobalErrors().stream()
                .map(error -> new FieldViolation(error.getObjectName(), error.getDefaultMessage()));

        ProblemDetail body = validationProblem(ex.getBody(), Stream.concat(fieldErrors, globalErrors));
        return handleExceptionInternal(ex, body, headers, status, request);
    }

    /**
     * Parâmetros do método inválidos (ex.: {@code @Max} em query param).
     */
    @Override
    protected ResponseEntity<Object> handleHandlerMethodValidationException(HandlerMethodValidationException ex,
                                                                            HttpHeaders headers, HttpStatusCode status,
                                                                            WebRequest request) {
        Stream<FieldViolation> violations = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream()
                        .map(error -> new FieldViolation(
                                result.getMethodParameter().getParameterName(),
                                error.getDefaultMessage())));

        ProblemDetail body = validationProblem(ex.getBody(), violations);
        return handleExceptionInternal(ex, body, headers, status, request);
    }

    private static ProblemDetail validationProblem(ProblemDetail body, Stream<FieldViolation> violations) {
        List<FieldViolation> errors = violations
                .sorted(Comparator.comparing(FieldViolation::field, Comparator.nullsLast(Comparator.naturalOrder())))
                .toList();
        body.setTitle(VALIDATION_TITLE);
        body.setDetail(errors.size() == 1
                ? "1 campo inválido."
                : errors.size() + " campos inválidos.");
        body.setProperty("errors", errors);
        return body;
    }

    private static String message(FieldError error) {
        // Falha de conversão de tipo (ex.: texto em campo numérico) não tem mensagem amigável por padrão.
        return error.isBindingFailure() ? "valor em formato inválido" : error.getDefaultMessage();
    }
}
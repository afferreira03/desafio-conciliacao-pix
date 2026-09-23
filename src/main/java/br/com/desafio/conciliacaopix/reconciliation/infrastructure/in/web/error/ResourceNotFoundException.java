package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponseException;

/**
 * 404 renderizado como ProblemDetail (RFC 9457) pelo {@link ApiExceptionHandler}.
 */
public class ResourceNotFoundException extends ErrorResponseException {

    public ResourceNotFoundException(String resource, String identifier) {
        super(HttpStatus.NOT_FOUND, problem(resource, identifier), null);
    }

    private static ProblemDetail problem(String resource, String identifier) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.NOT_FOUND,
                resource + " não encontrado(a) para o identificador " + identifier + "."
        );
        problem.setTitle("Recurso não encontrado");
        return problem;
    }
}

package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringDocConfig {

    private static final String DESCRIPTION = """
            API de consulta da conciliação automática de pagamentos Pix.

            Cada transação Pix recebida é conciliada com uma fatura e classificada como:
            - **CONCILIADO** — fatura encontrada e valor confere;
            - **PENDENTE** — nenhuma fatura correspondente encontrada;
            - **INCONSISTENTE** — fatura encontrada, mas com divergência (valor, já paga, expirada, cancelada ou ambígua).

            Erros seguem o formato ProblemDetail (RFC 9457).

            **Sem autenticação nesta versão** — controle de acesso documentado como trade-off no README.
            """;

    @Bean
    public OpenAPI conciliacaoPixOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("API de Conciliação Pix")
                        .version("v1")
                        .description(DESCRIPTION));
    }
}

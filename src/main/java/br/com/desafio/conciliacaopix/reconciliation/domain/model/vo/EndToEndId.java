package br.com.desafio.conciliacaopix.reconciliation.domain.model.vo;

import java.util.Objects;
import java.util.regex.Pattern;

public record EndToEndId(String value) {

    private static final Pattern PATTERN = Pattern.compile("^E[a-zA-Z0-9]{31}$");

    public EndToEndId {
        if (Objects.isNull(value) || value.isBlank()) {
            throw new IllegalArgumentException("EndToEndId não pode ser nulo ou vazio");
        }

        if (!PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException("EndToEndId com formato inválido. Deve ter 32 caracteres começando com a letra 'E': " + value);
        }
    }

    public static EndToEndId of(String value) {
        return new EndToEndId(value);
    }
}

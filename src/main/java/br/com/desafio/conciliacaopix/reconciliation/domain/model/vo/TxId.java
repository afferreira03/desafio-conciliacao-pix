package br.com.desafio.conciliacaopix.reconciliation.domain.model.vo;

import java.util.Objects;
import java.util.regex.Pattern;

public record TxId(String value) {

    private static final Pattern PATTERN = Pattern.compile("^[a-zA-Z0-9]{1,35}$");

    public TxId {
        if (Objects.isNull(value) || value.isBlank()) {
            throw new IllegalArgumentException("TxId não pode ser vazio ou nulo");
        }

        if (!PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException("TxId com formato inválido. Precisa ter entre 1 e 35 caracteres: " + value);
        }
    }

    public static TxId of(String value) {
        return new TxId(value);
    }
}

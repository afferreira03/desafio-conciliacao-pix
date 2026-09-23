package br.com.desafio.conciliacaopix.reconciliation.application.model;

public record PageQuery(int page, int size) {

    public static final int MAX_SIZE = 100;

    public PageQuery {
        if (page < 0) {
            throw new IllegalArgumentException("Página não pode ser negativa.");
        }
        if (size < 1 || size > MAX_SIZE) {
            throw new IllegalArgumentException("Tamanho da página deve estar entre 1 e " + MAX_SIZE + ".");
        }
    }

    public long offset() {
        return (long) page * size;
    }
}

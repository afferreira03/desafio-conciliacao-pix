package br.com.desafio.conciliacaopix.reconciliation.application.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public record PageResult<T>(List<T> items, int page, int size, long totalElements) {

    public PageResult {
        items = List.copyOf(Objects.requireNonNull(items, "Itens não podem ser nulos."));
    }

    public int totalPages() {
        return size == 0 ? 0 : (int) Math.ceil((double) totalElements / size);
    }

    public <R> PageResult<R> map(Function<T, R> mapper) {
        return new PageResult<>(items.stream().map(mapper).toList(), page, size, totalElements);
    }
}

package br.com.desafio.conciliacaopix.reconciliation.domain.model.vo;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

public record Money(BigDecimal value) implements Comparable<Money> {

    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_EVEN;
    private static final int DEFAULT_SCALE = 2;
    public static final Money ZERO = new Money(BigDecimal.ZERO);

    public Money {
        Objects.requireNonNull(value, "Valor não pode ser nulo.");

        if (value.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Valor monetário não pode ser negativo.");
        }

        value = value.setScale(DEFAULT_SCALE, ROUNDING_MODE);
    }

    public static Money of(BigDecimal amount) {
        return new Money(amount);
    }

    public static Money of(String amount) {
        return new Money(new BigDecimal(amount));
    }

    public static Money of(double amount) {
        return new Money(BigDecimal.valueOf(amount));
    }

    public boolean isEqualTo(Money other) {
        return other != null && this.value.compareTo(other.value) == 0;
    }

    public boolean isGreaterThan(Money other) {
        return other != null && this.value.compareTo(other.value) > 0;
    }

    public boolean isLessThan(Money other) {
        return other != null && this.value.compareTo(other.value) < 0;
    }

    public Money plus(Money other) {
        Objects.requireNonNull(other, "Valor não pode ser nulo.");
        return new Money(this.value.add(other.value));
    }

    @Override
    public int compareTo(Money other) {
        return this.value.compareTo(other.value);
    }
}


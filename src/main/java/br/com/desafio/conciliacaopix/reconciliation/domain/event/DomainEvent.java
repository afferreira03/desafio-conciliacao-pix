package br.com.desafio.conciliacaopix.reconciliation.domain.event;

import java.time.Instant;

public sealed interface DomainEvent permits PixReconciledEvent, PixInconsistentEvent, PixPendingEvent {
    Instant occurredOn();
}

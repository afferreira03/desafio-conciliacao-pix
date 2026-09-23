package br.com.desafio.conciliacaopix.reconciliation.application.port.out;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;

import java.time.Instant;

/**
 * Porta de métricas: mantém o serviço de aplicação livre de Micrometer/Spring.
 */
public interface ReconciliationMetricsPort {

    /**
     * Chamado após a persistência (commit) de uma nova conciliação.
     *
     * @param paymentTimestamp instante do pagamento Pix, base da latência ponta a ponta.
     */
    void recordReconciled(ReconciliationRecord reconciliationRecord, Instant paymentTimestamp);

    /**
     * Chamado quando o endToEndId já havia sido processado (redelivery).
     */
    void recordDuplicate();
}
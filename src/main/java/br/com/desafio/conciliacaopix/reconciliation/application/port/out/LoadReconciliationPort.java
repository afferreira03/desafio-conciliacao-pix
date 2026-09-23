package br.com.desafio.conciliacaopix.reconciliation.application.port.out;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;

import java.util.Optional;

public interface LoadReconciliationPort {
    Optional<ReconciliationRecord> findByEndToEndId(EndToEndId endToEndId);
}

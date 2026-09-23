package br.com.desafio.conciliacaopix.reconciliation.application.port.in;

import br.com.desafio.conciliacaopix.reconciliation.application.model.PageQuery;
import br.com.desafio.conciliacaopix.reconciliation.application.model.PageResult;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSearchCriteria;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;

import java.time.Instant;
import java.util.Optional;

public interface QueryReconciliationUseCase {

    Optional<ReconciliationRecord> findByEndToEndId(EndToEndId endToEndId);

    PageResult<ReconciliationRecord> search(ReconciliationSearchCriteria criteria, PageQuery pageQuery);

    ReconciliationSummary summarize(Instant from, Instant to);
}

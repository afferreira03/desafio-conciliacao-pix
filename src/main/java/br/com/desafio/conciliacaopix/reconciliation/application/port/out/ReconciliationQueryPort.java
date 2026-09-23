package br.com.desafio.conciliacaopix.reconciliation.application.port.out;

import br.com.desafio.conciliacaopix.reconciliation.application.model.PageQuery;
import br.com.desafio.conciliacaopix.reconciliation.application.model.PageResult;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSearchCriteria;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationTotals;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;

import java.time.Instant;
import java.util.List;

public interface ReconciliationQueryPort {

    PageResult<ReconciliationRecord> search(ReconciliationSearchCriteria criteria, PageQuery pageQuery);

    List<ReconciliationTotals> totals(Instant from, Instant to);
}

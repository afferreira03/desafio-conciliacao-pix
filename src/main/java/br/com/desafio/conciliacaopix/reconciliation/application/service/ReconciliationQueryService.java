package br.com.desafio.conciliacaopix.reconciliation.application.service;

import br.com.desafio.conciliacaopix.reconciliation.application.model.PageQuery;
import br.com.desafio.conciliacaopix.reconciliation.application.model.PageResult;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSearchCriteria;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary.ReasonSummary;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSummary.StatusSummary;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationTotals;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.QueryReconciliationUseCase;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.ReconciliationQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ReconciliationQueryService implements QueryReconciliationUseCase {

    private final LoadReconciliationPort loadReconciliationPort;
    private final ReconciliationQueryPort reconciliationQueryPort;

    public ReconciliationQueryService(LoadReconciliationPort loadReconciliationPort, ReconciliationQueryPort reconciliationQueryPort) {
        this.loadReconciliationPort = Objects.requireNonNull(loadReconciliationPort, "LoadReconciliationPort é obrigatório.");
        this.reconciliationQueryPort = Objects.requireNonNull(reconciliationQueryPort, "ReconciliationQueryPort é obrigatório.");
    }

    @Override
    public Optional<ReconciliationRecord> findByEndToEndId(EndToEndId endToEndId) {
        Objects.requireNonNull(endToEndId, "EndToEndId é obrigatório.");
        return loadReconciliationPort.findByEndToEndId(endToEndId);
    }

    @Override
    public PageResult<ReconciliationRecord> search(ReconciliationSearchCriteria criteria, PageQuery pageQuery) {
        Objects.requireNonNull(criteria, "Critérios de busca são obrigatórios.");
        Objects.requireNonNull(pageQuery, "Paginação é obrigatória.");
        return reconciliationQueryPort.search(criteria, pageQuery);
    }

    @Override
    public ReconciliationSummary summarize(Instant from, Instant to) {
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("O início do período ('from') deve ser anterior ao fim ('to').");
        }

        List<ReconciliationTotals> totals = reconciliationQueryPort.totals(from, to);

        List<StatusSummary> statuses = Arrays.stream(ReconciliationStatus.values())
                .map(status -> summarizeStatus(status, totals))
                .toList();

        long totalCount = statuses.stream().mapToLong(StatusSummary::count).sum();
        Money totalAmount = statuses.stream().map(StatusSummary::total).reduce(Money.ZERO, Money::plus);

        return new ReconciliationSummary(from, to, totalCount, totalAmount, statuses);
    }

    private StatusSummary summarizeStatus(ReconciliationStatus status, List<ReconciliationTotals> totals) {
        List<ReconciliationTotals> rows = totals.stream()
                .filter(row -> row.status() == status)
                .toList();

        long count = rows.stream().mapToLong(ReconciliationTotals::count).sum();
        Money total = rows.stream().map(ReconciliationTotals::total).reduce(Money.ZERO, Money::plus);

        List<ReasonSummary> reasons = rows.stream()
                .filter(row -> row.reason() != null)
                .map(row -> new ReasonSummary(row.reason(), row.count(), row.total()))
                .sorted(Comparator.comparingLong(ReasonSummary::count).reversed())
                .toList();

        return new StatusSummary(status, count, total, reasons);
    }
}

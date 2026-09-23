package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.adapter;

import br.com.desafio.conciliacaopix.reconciliation.application.model.PageQuery;
import br.com.desafio.conciliacaopix.reconciliation.application.model.PageResult;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationSearchCriteria;
import br.com.desafio.conciliacaopix.reconciliation.application.model.ReconciliationTotals;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.ReconciliationQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.ReconciliationRecord;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.ReconciliationDocument;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Component
public class ReconciliationQueryMongoAdapter implements ReconciliationQueryPort {

    private static final String STATUS = "status";
    private static final String REASON = "inconsistencyReason";
    private static final String CREATED_AT = "createdAt";
    private static final String TRANSACTION_AMOUNT = "transactionAmount";

    private final MongoTemplate mongoTemplate;

    public ReconciliationQueryMongoAdapter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public PageResult<ReconciliationRecord> search(ReconciliationSearchCriteria criteria, PageQuery pageQuery) {
        Criteria filter = buildFilter(criteria);

        Query pageSelection = new Query(filter)
                .with(Sort.by(Sort.Direction.DESC, CREATED_AT))
                .skip(pageQuery.offset())
                .limit(pageQuery.size());

        List<ReconciliationRecord> items = mongoTemplate.find(pageSelection, ReconciliationDocument.class)
                .stream()
                .map(ReconciliationDocument::toDomain)
                .toList();

        long total = mongoTemplate.count(new Query(filter), ReconciliationDocument.class);

        return new PageResult<>(items, pageQuery.page(), pageQuery.size(), total);
    }

    @Override
    public List<ReconciliationTotals> totals(Instant from, Instant to) {
        List<AggregationOperation> pipeline = new ArrayList<>();

        Criteria range = createdAtRange(from, to);
        if (range != null) {
            pipeline.add(Aggregation.match(range));
        }
        pipeline.add(Aggregation.group(STATUS, REASON)
                .count().as("count")
                .sum(TRANSACTION_AMOUNT).as("total"));

        return mongoTemplate.aggregate(Aggregation.newAggregation(pipeline), ReconciliationDocument.class, Document.class)
                .getMappedResults()
                .stream()
                .map(this::toTotals)
                .toList();
    }

    private Criteria buildFilter(ReconciliationSearchCriteria criteria) {
        List<Criteria> clauses = new ArrayList<>();

        if (criteria.status() != null) {
            clauses.add(Criteria.where(STATUS).is(criteria.status()));
        }
        if (criteria.reason() != null) {
            clauses.add(Criteria.where(REASON).is(criteria.reason()));
        }
        Criteria range = createdAtRange(criteria.from(), criteria.to());
        if (range != null) {
            clauses.add(range);
        }

        return clauses.isEmpty() ? new Criteria() : new Criteria().andOperator(clauses);
    }

    private Criteria createdAtRange(Instant from, Instant to) {
        if (from == null && to == null) {
            return null;
        }
        Criteria range = Criteria.where(CREATED_AT);
        if (from != null) {
            range = range.gte(from);
        }
        if (to != null) {
            range = range.lte(to);
        }
        return range;
    }

    private ReconciliationTotals toTotals(Document row) {
        Document id = row.get("_id", Document.class);
        String reason = id.getString(REASON);

        return new ReconciliationTotals(
                ReconciliationStatus.valueOf(id.getString(STATUS)),
                reason != null ? InconsistencyReason.valueOf(reason) : null,
                ((Number) row.get("count")).longValue(),
                Money.of(toBigDecimal(row.get("total")))
        );
    }

    private BigDecimal toBigDecimal(Object value) {
        return switch (value) {
            case null -> BigDecimal.ZERO;
            case Decimal128 decimal -> decimal.bigDecimalValue();
            case BigDecimal decimal -> decimal;
            default -> new BigDecimal(value.toString());
        };
    }
}

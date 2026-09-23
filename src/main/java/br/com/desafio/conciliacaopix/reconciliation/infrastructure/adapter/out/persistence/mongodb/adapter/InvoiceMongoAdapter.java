package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.adapter;

import br.com.desafio.conciliacaopix.reconciliation.application.exception.InvoiceAlreadyExistsException;
import br.com.desafio.conciliacaopix.reconciliation.application.model.InvoiceView;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.InvoiceQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.InvoiceDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.repository.SpringDataInvoiceRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Component
public class InvoiceMongoAdapter implements LoadInvoicePort, SaveInvoicePort, InvoiceQueryPort {

    private final SpringDataInvoiceRepository repository;

    public InvoiceMongoAdapter(SpringDataInvoiceRepository repository) {
        this.repository = repository;
    }

    @Override
    public Optional<Invoice> findByTxId(TxId txId) {
        return repository.findByTxId(txId.value()).map(InvoiceDocument::toDomain);
    }

    @Override
    public List<Invoice> findPendingCandidatesByFallback(String pixKey, Money amount, Instant paymentTimestamp, Duration window) {
        Instant startWindow = paymentTimestamp.minus(window);
        Instant endWindow = paymentTimestamp.plus(window);

        return repository.findCandidatesFallback(
                        pixKey,
                        amount.value(),
                        InvoiceStatus.ABERTA,
                        startWindow,
                        endWindow
                )
                .stream()
                .map(InvoiceDocument::toDomain)
                .toList();
    }

    /**
     * Fora de transação Mongo, então capturar a violação do índice único aqui é seguro
     * (diferente do fluxo de conciliação, em que o erro aborta a transação).
     */
    @Override
    public void save(Invoice invoice, String pixKey) {
        try {
            repository.insert(InvoiceDocument.fromDomain(invoice, pixKey));
        } catch (DuplicateKeyException e) {
            throw new InvoiceAlreadyExistsException(invoice.getTxId().value());
        }
    }

    @Override
    public Optional<InvoiceView> findViewByTxId(TxId txId) {
        return repository.findByTxId(txId.value())
                .map(doc -> new InvoiceView(
                        TxId.of(doc.getTxId()),
                        Money.of(doc.getAmount()),
                        doc.getStatus(),
                        doc.getPixKey(),
                        doc.getCreatedAt(),
                        doc.getExpiryDate()
                ));
    }
}

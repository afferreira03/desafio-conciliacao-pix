package br.com.desafio.conciliacaopix.reconciliation.application.port.out;

import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface LoadInvoicePort {
    Optional<Invoice> findByTxId(TxId txId);

    List<Invoice> findPendingCandidatesByFallback(String pixKey, Money amount, Instant paymentTimestamp, Duration window);
}

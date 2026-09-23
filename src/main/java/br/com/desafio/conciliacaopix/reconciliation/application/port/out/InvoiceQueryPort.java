package br.com.desafio.conciliacaopix.reconciliation.application.port.out;

import br.com.desafio.conciliacaopix.reconciliation.application.model.InvoiceView;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.util.Optional;

public interface InvoiceQueryPort {

    Optional<InvoiceView> findViewByTxId(TxId txId);
}

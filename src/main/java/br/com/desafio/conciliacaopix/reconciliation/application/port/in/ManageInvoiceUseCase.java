package br.com.desafio.conciliacaopix.reconciliation.application.port.in;

import br.com.desafio.conciliacaopix.reconciliation.application.model.InvoiceView;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.util.Optional;

public interface ManageInvoiceUseCase {

    InvoiceView create(CreateInvoiceCommand command);

    Optional<InvoiceView> findByTxId(TxId txId);
}

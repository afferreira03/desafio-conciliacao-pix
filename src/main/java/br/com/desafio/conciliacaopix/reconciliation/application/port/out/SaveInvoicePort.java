package br.com.desafio.conciliacaopix.reconciliation.application.port.out;

import br.com.desafio.conciliacaopix.reconciliation.application.exception.InvoiceAlreadyExistsException;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;

public interface SaveInvoicePort {

    /**
     * @throws InvoiceAlreadyExistsException se já existir fatura com o mesmo txId.
     */
    void save(Invoice invoice, String pixKey);
}

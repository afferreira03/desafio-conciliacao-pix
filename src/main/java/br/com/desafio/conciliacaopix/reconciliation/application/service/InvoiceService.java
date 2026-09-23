package br.com.desafio.conciliacaopix.reconciliation.application.service;

import br.com.desafio.conciliacaopix.reconciliation.application.model.InvoiceView;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.CreateInvoiceCommand;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ManageInvoiceUseCase;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.InvoiceQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;

import java.util.Objects;
import java.util.Optional;

public class InvoiceService implements ManageInvoiceUseCase {

    private final SaveInvoicePort saveInvoicePort;
    private final InvoiceQueryPort invoiceQueryPort;

    public InvoiceService(SaveInvoicePort saveInvoicePort, InvoiceQueryPort invoiceQueryPort) {
        this.saveInvoicePort = Objects.requireNonNull(saveInvoicePort, "SaveInvoicePort é obrigatório.");
        this.invoiceQueryPort = Objects.requireNonNull(invoiceQueryPort, "InvoiceQueryPort é obrigatório.");
    }

    @Override
    public InvoiceView create(CreateInvoiceCommand command) {
        Objects.requireNonNull(command, "Command não pode ser nulo.");

        Invoice invoice = Invoice.open(TxId.of(command.txId()), command.amount(), command.expiresAt());
        saveInvoicePort.save(invoice, command.pixKey());

        return new InvoiceView(
                invoice.getTxId(),
                invoice.getAmount(),
                invoice.getStatus(),
                command.pixKey(),
                invoice.getCreatedAt(),
                invoice.getExpirationDate()
        );
    }

    @Override
    public Optional<InvoiceView> findByTxId(TxId txId) {
        Objects.requireNonNull(txId, "TxId é obrigatório.");
        return invoiceQueryPort.findViewByTxId(txId);
    }
}

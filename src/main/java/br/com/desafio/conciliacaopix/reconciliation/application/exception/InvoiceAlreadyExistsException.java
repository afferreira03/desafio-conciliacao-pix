package br.com.desafio.conciliacaopix.reconciliation.application.exception;

public class InvoiceAlreadyExistsException extends RuntimeException {

    public InvoiceAlreadyExistsException(String txId) {
        super("Já existe uma fatura com o txId " + txId + ".");
    }
}

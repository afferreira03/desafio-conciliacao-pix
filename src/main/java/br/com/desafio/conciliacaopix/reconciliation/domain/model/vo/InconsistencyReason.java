package br.com.desafio.conciliacaopix.reconciliation.domain.model.vo;

public enum InconsistencyReason {

    INVOICE_NOT_FOUND("Fatura não encontrada."),
    AMOUNT_MISMATCH("Valores da operação e da fatura não correspondem."),
    INVOICE_ALREADY_PAID("Fatura já foi paga."),
    INVOICE_CANCELLED("Fatura foi cancelada."),
    INVOICE_EXPIRED("Fatura está vencida."),
    MULTIPLE_INVOICES_MATCHED("Múltiplas faturas encontradas para os mesmos parâmetros na janela de tempo.");

    private final String errorMessage;

    InconsistencyReason(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}

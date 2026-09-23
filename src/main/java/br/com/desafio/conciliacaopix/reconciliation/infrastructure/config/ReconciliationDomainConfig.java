package br.com.desafio.conciliacaopix.reconciliation.infrastructure.config;

import br.com.desafio.conciliacaopix.reconciliation.application.port.out.InvoiceQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.ReconciliationMetricsPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.ReconciliationQueryPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.service.InvoiceService;
import br.com.desafio.conciliacaopix.reconciliation.application.service.ReconcilePixTransactionService;
import br.com.desafio.conciliacaopix.reconciliation.application.service.ReconciliationQueryService;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ReconciliationDomainConfig {

    @Bean
    public ReconciliationEngine reconciliationEngine() {
        return new ReconciliationEngine();
    }

    @Bean
    public ReconcilePixTransactionService reconcilePixTransactionService(
            ReconciliationEngine reconciliationEngine,
            LoadInvoicePort loadInvoicePort,
            SaveReconciliationPort saveReconciliationPort,
            LoadReconciliationPort reconciliationPort,
            ReconciliationMetricsPort metricsPort) {
        return new ReconcilePixTransactionService(
                reconciliationEngine,
                loadInvoicePort,
                saveReconciliationPort,
                reconciliationPort,
                metricsPort
        );
    }

    @Bean
    public ReconciliationQueryService reconciliationQueryService(
            LoadReconciliationPort loadReconciliationPort,
            ReconciliationQueryPort reconciliationQueryPort) {
        return new ReconciliationQueryService(loadReconciliationPort, reconciliationQueryPort);
    }

    @Bean
    public InvoiceService invoiceService(SaveInvoicePort saveInvoicePort, InvoiceQueryPort invoiceQueryPort) {
        return new InvoiceService(saveInvoicePort, invoiceQueryPort);
    }
}

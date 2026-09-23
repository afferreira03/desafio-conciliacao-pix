package br.com.desafio.conciliacaopix.reconciliation.infrastructure.config;

import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.application.service.ReconcilePixTransactionService;
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
            LoadReconciliationPort reconciliationPort) {
        return new ReconcilePixTransactionService(
                reconciliationEngine,
                loadInvoicePort,
                saveReconciliationPort,
                reconciliationPort
        );
    }
}

package br.com.desafio.conciliacaopix.integration;

import br.com.desafio.conciliacaopix.reconciliation.application.port.in.CreateInvoiceCommand;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ManageInvoiceUseCase;
import br.com.desafio.conciliacaopix.reconciliation.application.port.in.ReconciliationPixCommand;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.LoadInvoicePort;
import br.com.desafio.conciliacaopix.reconciliation.application.port.out.SaveReconciliationPort;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.Invoice;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.PixTransaction;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InconsistencyReason;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.InvoiceStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.Money;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.ReconciliationStatus;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.TxId;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationEngine;
import br.com.desafio.conciliacaopix.reconciliation.domain.service.ReconciliationResult;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.metrics.MicrometerReconciliationMetricsAdapter;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.InvoiceDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.OutboxEventStatus;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document.ReconciliationDocument;
import br.com.desafio.conciliacaopix.reconciliation.infrastructure.config.KafkaTopicsProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * Fluxo completo contra MongoDB (replica set) e Redpanda reais: consumo Kafka → transação Mongo (registro + fatura +
 * outbox) → relay do outbox → tópico de resultado; idempotência; DLT; e o rollback real do compare-and-set.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ReconciliationFlowIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Logger LOG = LoggerFactory.getLogger(ReconciliationFlowIT.class);

    @Autowired
    private ManageInvoiceUseCase invoiceUseCase;
    @Autowired
    private LoadInvoicePort loadInvoicePort;
    @Autowired
    private SaveReconciliationPort saveReconciliationPort;
    @Autowired
    private ReconciliationEngine engine;
    @Autowired
    private MongoTemplate mongo;
    @Autowired
    private MeterRegistry meterRegistry;
    @Autowired
    private KafkaTopicsProperties topics;
    @Autowired
    @Qualifier("outboxKafkaTemplate")
    private KafkaTemplate<String, String> kafka;

    @Test
    @DisplayName("Pix com fatura aberta: CONCILIADO, fatura PAGA, outbox SENT e evento no tópico de resultado")
    void reconcilesEndToEnd() throws Exception {
        String txId = newTxId();
        createInvoice(txId, "150.00");
        String e2e = newEndToEndId();

        kafka.send(topics.pixTransactions(), txId, pixJson(e2e, txId, "150.00")).get();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(reconciliation(e2e))
                .isNotNull()
                .extracting(ReconciliationDocument::getStatus).isEqualTo(ReconciliationStatus.CONCILIADO));
        assertThat(invoice(txId).getStatus()).isEqualTo(InvoiceStatus.PAGA);

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(outbox(e2e))
                .singleElement()
                .extracting(OutboxEventDocument::getStatus).isEqualTo(OutboxEventStatus.SENT));

        ConsumerRecord<String, String> result = pollUntil(topics.pixReconciliationResult(), r -> e2e.equals(r.key()));
        assertThat(result.value()).contains(e2e);
    }

    @Test
    @DisplayName("Mensagem reentregue (mesmo endToEndId) gera uma única conciliação")
    void redeliveryIsIdempotent() throws Exception {
        double duplicatesBefore = duplicates();
        String txId = newTxId(); // sem fatura → PENDENTE
        String e2e = newEndToEndId();
        String payload = pixJson(e2e, txId, "10.00");

        kafka.send(topics.pixTransactions(), txId, payload).get();
        kafka.send(topics.pixTransactions(), txId, payload).get();

        await().atMost(TIMEOUT).until(() -> duplicates() >= duplicatesBefore + 1);
        assertThat(mongo.count(query(where("endToEndId").is(e2e)), ReconciliationDocument.class)).isEqualTo(1);
        assertThat(reconciliation(e2e).getStatus()).isEqualTo(ReconciliationStatus.PENDENTE);
        assertThat(outbox(e2e)).hasSize(1);
    }

    @Test
    @DisplayName("Mensagem inválida (JSON malformado) vai para o DLT sem conciliação")
    void poisonMessageGoesToDeadLetterTopic() throws Exception {
        String key = "poison-" + UUID.randomUUID();
        String payload = "{isto não é json";

        kafka.send(topics.pixTransactions(), key, payload).get();

        ConsumerRecord<String, String> dead = pollUntil(topics.pixTransactionsDlt(), r -> key.equals(r.key()));
        assertThat(dead.value()).isEqualTo(payload);
    }

    @Test
    @DisplayName("Compare-and-set: segundo pagamento com visão desatualizada da fatura faz rollback de toda a transação")
    void compareAndSetRollsBackWholeTransaction() {
        String txId = newTxId();
        createInvoice(txId, "50.00");
        // Duas leituras da mesma fatura ABERTA — simula dois consumidores concorrentes.
        Invoice firstView = loadInvoicePort.findByTxId(TxId.of(txId)).orElseThrow();
        Invoice staleView = loadInvoicePort.findByTxId(TxId.of(txId)).orElseThrow();
        String firstE2e = newEndToEndId();
        String secondE2e = newEndToEndId();

        ReconciliationResult first = engine.reconcile(pix(firstE2e, txId, "50.00"), Optional.of(firstView));
        saveReconciliationPort.save(first.record(), first.updatedInvoice(), first.event());

        ReconciliationResult second = engine.reconcile(pix(secondE2e, txId, "50.00"), Optional.of(staleView));
        assertThat(second.updatedInvoice()).as("a visão desatualizada ainda enxerga a fatura ABERTA").isPresent();

        assertThatThrownBy(() -> saveReconciliationPort.save(second.record(), second.updatedInvoice(), second.event()))
                .isInstanceOf(OptimisticLockingFailureException.class);

        // O registro de conciliação e o outbox foram gravados ANTES do update condicional na mesma transação:
        // se não existem, o rollback do Mongo realmente aconteceu.
        assertThat(reconciliation(secondE2e)).isNull();
        assertThat(outbox(secondE2e)).isEmpty();
        assertThat(reconciliation(firstE2e)).isNotNull();
        assertThat(invoice(txId).getStatus()).isEqualTo(InvoiceStatus.PAGA);
    }

    @Test
    @DisplayName("Compare-and-set sob concorrência real: 6 pagamentos simultâneos da mesma fatura conciliam exatamente um")
    void concurrentPaymentsForSameInvoiceConcileExactlyOnce() throws Exception {
        String txId = newTxId();
        createInvoice(txId, "75.00");
        int payments = 6; // = partições de pix.transactions = threads do consumer
        double listenerFailuresBefore = listenerFailures();

        // Cada pagamento vai para uma partição diferente, ignorando de propósito a chave txId: as 6 threads do
        // consumer disputam a mesma fatura ao mesmo tempo (ex.: um produtor que não particiona por txId).
        for (int partition = 0; partition < payments; partition++) {
            String e2e = newEndToEndId();
            kafka.send(topics.pixTransactions(), partition, e2e, pixJson(e2e, txId, "75.00"));
        }
        kafka.flush();

        // Qualquer que seja a intercalação: um vence o compare-and-set; os demais fazem rollback, são reprocessados
        // pelo retry e encontram a fatura PAGA. Se algum esgotasse as retentativas, iria ao DLT sem registro e a
        // contagem abaixo falharia.
        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<ReconciliationDocument> records = mongo.find(query(where("txId").is(txId)), ReconciliationDocument.class);
            assertThat(records).hasSize(payments);
            assertThat(records).filteredOn(r -> r.getStatus() == ReconciliationStatus.CONCILIADO).hasSize(1);
            assertThat(records).filteredOn(r -> r.getStatus() == ReconciliationStatus.INCONSISTENTE)
                    .hasSize(payments - 1)
                    .allSatisfy(r -> assertThat(r.getInconsistencyReason()).isEqualTo(InconsistencyReason.INVOICE_ALREADY_PAID));
        });
        assertThat(invoice(txId).getStatus()).isEqualTo(InvoiceStatus.PAGA);

        // Evidência (sem asserção, para não tornar o teste instável): tentativas que falharam por conflito e foram
        // reprocessadas pelo retry. > 0 mostra que a disputa realmente aconteceu nesta execução.
        LOG.info("Compare-and-set concorrente: {} tentativa(s) com falha reprocessada(s) pelo retry",
                (long) (listenerFailures() - listenerFailuresBefore));
    }

    // --- helpers ---

    private double listenerFailures() {
        return meterRegistry.find("spring.kafka.listener").tag("result", "failure").timers().stream()
                .mapToDouble(timer -> timer.count())
                .sum();
    }

    private void createInvoice(String txId, String amount) {
        invoiceUseCase.create(new CreateInvoiceCommand(txId, Money.of(new BigDecimal(amount)), "it@demo.com",
                Instant.now().plus(1, ChronoUnit.DAYS)));
    }

    private ReconciliationDocument reconciliation(String e2e) {
        return mongo.findOne(query(where("endToEndId").is(e2e)), ReconciliationDocument.class);
    }

    private InvoiceDocument invoice(String txId) {
        return mongo.findOne(query(where("txId").is(txId)), InvoiceDocument.class);
    }

    private List<OutboxEventDocument> outbox(String e2e) {
        return mongo.find(query(where("endToEndId").is(e2e)), OutboxEventDocument.class);
    }

    private double duplicates() {
        return meterRegistry.get(MicrometerReconciliationMetricsAdapter.DUPLICATE_COUNTER).counter().count();
    }

    private static PixTransaction pix(String e2e, String txId, String amount) {
        return new ReconciliationPixCommand(e2e, txId, Money.of(new BigDecimal(amount)), Instant.now(), "it@demo.com")
                .toDomain();
    }

    private static String pixJson(String e2e, String txId, String amount) {
        return """
                {"endToEndId":"%s","txId":"%s","transactionAmount":%s,"paymentTimestamp":"%s","pixKey":"it@demo.com"}"""
                .formatted(e2e, txId, amount, Instant.now());
    }

    private static String newEndToEndId() {
        return "E00000000202609231200" + randomAlnum(11);
    }

    private static String newTxId() {
        return "IT" + randomAlnum(20);
    }

    private static String randomAlnum(int length) {
        return UUID.randomUUID().toString().replace("-", "").substring(0, length).toUpperCase();
    }

    private static ConsumerRecord<String, String> pollUntil(String topic, Predicate<ConsumerRecord<String, String>> match) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TestcontainersConfiguration.REDPANDA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(TIMEOUT);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (match.test(record)) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("Nenhuma mensagem correspondente em '" + topic + "' em " + TIMEOUT);
    }
}
package br.com.desafio.conciliacaopix.loadtest;

import br.com.desafio.conciliacaopix.loadtest.LoadScenarioPlan.PixMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Publica as mensagens Pix com um {@link KafkaProducer} puro (sem Spring), de forma totalmente assíncrona:
 * {@code send()} sem bloquear por mensagem e um único {@code flush()} ao final.
 * <p>
 * Chave = {@code txId} (ou {@code endToEndId} sem txId): todas as mensagens de uma fatura caem na mesma partição, em
 * ordem — o que torna determinísticos os cenários "segundo pagamento" e "reenvio", e espelha o argumento de
 * particionamento em produção.
 */
public final class PixLoadGenerator {

    public record Result(int sent, int failed, Duration elapsed) {
        public double rate() {
            return sent / InvoiceSeeder.seconds(elapsed);
        }
    }

    private final String bootstrapServers;
    private final String topic;
    private final int ratePerSecond;

    /**
     * @param ratePerSecond 0 = burst (o mais rápido possível); &gt; 0 = taxa de chegada constante, para medir a
     *                      latência quando a chegada fica abaixo da capacidade de processamento.
     */
    public PixLoadGenerator(String bootstrapServers, String topic, int ratePerSecond) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.ratePerSecond = ratePerSecond;
    }

    public Result publish(List<PixMessage> phaseOne, List<PixMessage> phaseTwo) {
        AtomicInteger failed = new AtomicInteger();
        AtomicReference<Exception> firstError = new AtomicReference<>();
        Map<String, String> payloadByEndToEndId = new HashMap<>();
        int sent = 0;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            long start = System.nanoTime();

            for (PixMessage message : phaseOne) {
                pace(start, sent);
                String payload = toJson(message, Instant.now());
                payloadByEndToEndId.put(message.endToEndId(), payload);
                send(producer, message.key(), payload, failed, firstError);
                sent++;
            }
            for (PixMessage message : phaseTwo) {
                pace(start, sent);
                String payload = message.duplicateOf() != null
                        ? payloadByEndToEndId.get(message.duplicateOf()) // reenvio byte a byte idêntico
                        : toJson(message, Instant.now());
                send(producer, message.key(), payload, failed, firstError);
                sent++;
            }

            producer.flush();
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            if (firstError.get() != null) {
                System.err.println("  primeiro erro de envio: " + firstError.get());
            }
            return new Result(sent - failed.get(), failed.get(), elapsed);
        }
    }

    /** Espera até o instante agendado da mensagem {@code index} (agenda absoluta: não acumula atraso). */
    private void pace(long startNanos, int index) {
        if (ratePerSecond <= 0) {
            return;
        }
        long due = startNanos + index * 1_000_000_000L / ratePerSecond;
        long wait = due - System.nanoTime();
        if (wait > 0) {
            LockSupport.parkNanos(wait);
        }
    }

    private void send(KafkaProducer<String, String> producer, String key, String payload,
                      AtomicInteger failed, AtomicReference<Exception> firstError) {
        producer.send(new ProducerRecord<>(topic, key, payload), (metadata, exception) -> {
            if (exception != null) {
                failed.incrementAndGet();
                firstError.compareAndSet(null, exception);
            }
        });
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65_536);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    /** Mesmo formato de {@code PixTransactionEventDto}. */
    static String toJson(PixMessage message, Instant paymentTimestamp) {
        return """
                {"endToEndId":"%s","txId":%s,"transactionAmount":%s,"paymentTimestamp":"%s","pixKey":"%s"}"""
                .formatted(
                        message.endToEndId(),
                        message.txId() != null ? "\"" + message.txId() + "\"" : "null",
                        message.amount().toPlainString(),
                        paymentTimestamp,
                        message.pixKey());
    }
}
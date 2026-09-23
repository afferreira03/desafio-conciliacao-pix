package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka.dto.PixTransactionEventDto;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Coloca no MDC os identificadores da mensagem em processamento, para correlacionar todos os logs de um Pix
 * (consumer, serviço, retry e envio ao DLT).
 * <p>
 * As chaves são limpas em {@link #clearThreadState}, e não em {@code success/failure}: o spring-kafka chama esses
 * métodos <em>antes</em> do error handler, e o log de "retentativas esgotadas → DLT" precisa manter a correlação.
 * Como {@link #intercept} sobrescreve as chaves a cada mensagem, nada vaza de uma mensagem para outra.
 * <p>
 * Só identificadores técnicos entram no MDC — nunca a chave Pix (dado pessoal).
 */
public class MdcRecordInterceptor implements RecordInterceptor<String, PixTransactionEventDto> {

    public static final String END_TO_END_ID = "endToEndId";
    public static final String TX_ID = "txId";
    public static final String KAFKA = "kafka";

    @Override
    public ConsumerRecord<String, PixTransactionEventDto> intercept(ConsumerRecord<String, PixTransactionEventDto> record,
                                                                   Consumer<String, PixTransactionEventDto> consumer) {
        // Valor nulo = falha de desserialização (ErrorHandlingDeserializer): ainda há chave e posição no tópico.
        putOrRemove(END_TO_END_ID, record.value() != null ? record.value().endToEndId() : null);
        putOrRemove(TX_ID, record.key());
        MDC.put(KAFKA, record.topic() + "-" + record.partition() + "@" + record.offset());
        return record;
    }

    @Override
    public void clearThreadState(Consumer<?, ?> consumer) {
        MDC.remove(END_TO_END_ID);
        MDC.remove(TX_ID);
        MDC.remove(KAFKA);
    }

    private static void putOrRemove(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }
}
package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka;

import br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.messaging.kafka.dto.PixTransactionEventDto;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MdcRecordInterceptorTest {

    private static final String E2E_ID = "E0000000020260919123456789012345";

    private final MdcRecordInterceptor interceptor = new MdcRecordInterceptor();

    @SuppressWarnings("unchecked")
    private final Consumer<String, PixTransactionEventDto> consumer = mock(Consumer.class);

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    private ConsumerRecord<String, PixTransactionEventDto> record(String key, PixTransactionEventDto value, long offset) {
        return new ConsumerRecord<>("pix.transactions", 3, offset, key, value);
    }

    private PixTransactionEventDto pix(String endToEndId) {
        return new PixTransactionEventDto(endToEndId, "TX123", new BigDecimal("10.00"), Instant.now(), "user@email.com");
    }

    @Test
    @DisplayName("Deve colocar endToEndId, txId e posição no tópico no MDC e devolver a mesma mensagem")
    void shouldPutIdentifiersInMdc() {
        var record = record("TX123", pix(E2E_ID), 42);

        var returned = interceptor.intercept(record, consumer);

        assertThat(returned).isSameAs(record);
        assertThat(MDC.get(MdcRecordInterceptor.END_TO_END_ID)).isEqualTo(E2E_ID);
        assertThat(MDC.get(MdcRecordInterceptor.TX_ID)).isEqualTo("TX123");
        assertThat(MDC.get(MdcRecordInterceptor.KAFKA)).isEqualTo("pix.transactions-3@42");
    }

    @Test
    @DisplayName("Mensagem não desserializada (valor nulo) mantém chave e posição, sem endToEndId de mensagem anterior")
    void shouldHandleNullValueWithoutLeakingPreviousEndToEndId() {
        interceptor.intercept(record("TX123", pix(E2E_ID), 1), consumer);

        interceptor.intercept(record("LIXO", null, 2), consumer);

        assertThat(MDC.get(MdcRecordInterceptor.END_TO_END_ID)).isNull();
        assertThat(MDC.get(MdcRecordInterceptor.TX_ID)).isEqualTo("LIXO");
        assertThat(MDC.get(MdcRecordInterceptor.KAFKA)).isEqualTo("pix.transactions-3@2");
    }

    @Test
    @DisplayName("Não deve colocar a chave Pix (dado pessoal) no MDC")
    void shouldNotPutPixKeyInMdc() {
        interceptor.intercept(record("TX123", pix(E2E_ID), 1), consumer);

        assertThat(MDC.getCopyOfContextMap()).doesNotContainValue("user@email.com");
    }

    @Test
    @DisplayName("clearThreadState deve remover as chaves de correlação")
    void shouldClearKeys() {
        interceptor.intercept(record("TX123", pix(E2E_ID), 1), consumer);

        interceptor.clearThreadState(consumer);

        assertThat(MDC.get(MdcRecordInterceptor.END_TO_END_ID)).isNull();
        assertThat(MDC.get(MdcRecordInterceptor.TX_ID)).isNull();
        assertThat(MDC.get(MdcRecordInterceptor.KAFKA)).isNull();
    }
}
package br.com.desafio.conciliacaopix.loadtest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Plano de uma execução do teste de carga: faturas a semear, mensagens Pix a publicar (em duas fases, para garantir
 * a ordem "original → reenvio/segundo pagamento" na mesma partição) e o resultado esperado.
 */
public final class LoadScenarioPlan {

    public enum Scenario {
        MATCH, AMOUNT_MISMATCH, SECOND_PAYMENT, UNKNOWN_TXID, FALLBACK, REDELIVERY
    }

    /** Fatura a criar via {@code POST /api/v1/invoices}. {@code expectPaid} = deve terminar {@code PAGA}. */
    public record InvoiceSeed(String txId, BigDecimal amount, String pixKey, boolean expectPaid) {
    }

    /**
     * Mensagem Pix. {@code duplicateOf} != null indica reenvio exato (mesmo payload) de outra mensagem.
     * O {@code paymentTimestamp} é definido no momento do envio, para a latência medir fila + processamento.
     */
    public record PixMessage(Scenario scenario, String endToEndId, String txId, BigDecimal amount, String pixKey,
                             String duplicateOf) {
        public String key() {
            return txId != null ? txId : endToEndId;
        }
    }

    public record Expected(long records, long conciliado, long pendente, long inconsistente,
                           long amountMismatch, long alreadyPaid, long invoicesPaid, long messages) {
    }

    private static final String ISPB = "00000000";
    private static final String ALNUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final String GENERIC_PIX_KEY = "loja@demo.com";

    private final String runId;
    private final List<InvoiceSeed> invoices = new ArrayList<>();
    private final List<PixMessage> phaseOne = new ArrayList<>();
    private final List<PixMessage> phaseTwo = new ArrayList<>();
    private final Map<Scenario, Integer> counts = new LinkedHashMap<>();
    private final Random random;
    private final Set<String> usedEndToEndIds = new HashSet<>();
    private final String e2eTimestamp;

    private LoadScenarioPlan(int total, long seed) {
        this.random = new Random(seed);
        this.runId = Long.toString(System.currentTimeMillis() / 1000, 36).toUpperCase();
        this.e2eTimestamp = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));

        int mismatch = pct(total, 10);
        int second = pct(total, 5);
        int unknown = pct(total, 10);
        int fallback = pct(total, 5);
        int redelivery = pct(total, 5);
        int match = total - mismatch - second - unknown - fallback - redelivery;
        if (match < Math.max(second, redelivery) * 2) {
            throw new IllegalArgumentException("Total muito pequeno para o mix de cenários: " + total);
        }

        counts.put(Scenario.MATCH, match);
        counts.put(Scenario.AMOUNT_MISMATCH, mismatch);
        counts.put(Scenario.SECOND_PAYMENT, second);
        counts.put(Scenario.UNKNOWN_TXID, unknown);
        counts.put(Scenario.FALLBACK, fallback);
        counts.put(Scenario.REDELIVERY, redelivery);

        build(match, mismatch, second, unknown, fallback, redelivery);
    }

    public static LoadScenarioPlan create(int total) {
        return new LoadScenarioPlan(total, System.nanoTime());
    }

    private void build(int match, int mismatch, int second, int unknown, int fallback, int redelivery) {
        int seq = 0;
        List<PixMessage> matches = new ArrayList<>(match);

        for (int i = 0; i < match; i++) {
            String txId = txId("M", seq++);
            BigDecimal amount = randomAmount();
            invoices.add(new InvoiceSeed(txId, amount, GENERIC_PIX_KEY, true));
            PixMessage msg = new PixMessage(Scenario.MATCH, newEndToEndId(), txId, amount, GENERIC_PIX_KEY, null);
            matches.add(msg);
            phaseOne.add(msg);
        }

        for (int i = 0; i < mismatch; i++) {
            String txId = txId("D", seq++);
            BigDecimal amount = randomAmount();
            invoices.add(new InvoiceSeed(txId, amount, GENERIC_PIX_KEY, false));
            BigDecimal delta = BigDecimal.valueOf(1 + random.nextInt(1000), 2); // 0.01 – 10.00
            BigDecimal paid = random.nextBoolean() ? amount.add(delta) : amount.subtract(delta);
            phaseOne.add(new PixMessage(Scenario.AMOUNT_MISMATCH, newEndToEndId(), txId, paid, GENERIC_PIX_KEY, null));
        }

        for (int i = 0; i < unknown; i++) {
            phaseOne.add(new PixMessage(Scenario.UNKNOWN_TXID, newEndToEndId(), txId("U", seq++), randomAmount(),
                    GENERIC_PIX_KEY, null));
        }

        for (int i = 0; i < fallback; i++) {
            String pixKey = "lt" + runId.toLowerCase() + "f" + i + "@demo.com";
            BigDecimal amount = randomAmount();
            // A fatura precisa de txId na API, mas a mensagem Pix não o envia → conciliação por chave + valor + janela.
            invoices.add(new InvoiceSeed(txId("F", seq++), amount, pixKey, true));
            phaseOne.add(new PixMessage(Scenario.FALLBACK, newEndToEndId(), null, amount, pixKey, null));
        }

        // Fase 2: segundo pagamento (novo endToEndId) de faturas já pagas e reenvios exatos.
        // Mesma chave (txId) da mensagem original → mesma partição → processado depois dela.
        for (int i = 0; i < second; i++) {
            PixMessage original = matches.get(i);
            phaseTwo.add(new PixMessage(Scenario.SECOND_PAYMENT, newEndToEndId(), original.txId(), original.amount(),
                    GENERIC_PIX_KEY, null));
        }
        for (int i = 0; i < redelivery; i++) {
            PixMessage original = matches.get(matches.size() - 1 - i);
            phaseTwo.add(new PixMessage(Scenario.REDELIVERY, original.endToEndId(), original.txId(), original.amount(),
                    original.pixKey(), original.endToEndId()));
        }

        Collections.shuffle(phaseOne, random);
        Collections.shuffle(phaseTwo, random);
    }

    public Expected expected() {
        long match = counts.get(Scenario.MATCH);
        long mismatch = counts.get(Scenario.AMOUNT_MISMATCH);
        long second = counts.get(Scenario.SECOND_PAYMENT);
        long unknown = counts.get(Scenario.UNKNOWN_TXID);
        long fallback = counts.get(Scenario.FALLBACK);
        long redelivery = counts.get(Scenario.REDELIVERY);
        long messages = match + mismatch + second + unknown + fallback + redelivery;
        return new Expected(
                messages - redelivery,
                match + fallback,
                unknown,
                mismatch + second,
                mismatch,
                second,
                match + fallback,
                messages
        );
    }

    public String runId() {
        return runId;
    }

    public Map<Scenario, Integer> counts() {
        return Collections.unmodifiableMap(counts);
    }

    public List<InvoiceSeed> invoices() {
        return Collections.unmodifiableList(invoices);
    }

    public List<PixMessage> phaseOne() {
        return Collections.unmodifiableList(phaseOne);
    }

    public List<PixMessage> phaseTwo() {
        return Collections.unmodifiableList(phaseTwo);
    }

    private String txId(String prefix, int seq) {
        return "LT" + runId + prefix + seq; // alfanumérico, bem abaixo de 35 caracteres
    }

    private String newEndToEndId() {
        String id;
        do {
            StringBuilder sb = new StringBuilder("E").append(ISPB).append(e2eTimestamp);
            for (int i = 0; i < 11; i++) {
                sb.append(ALNUM.charAt(random.nextInt(ALNUM.length())));
            }
            id = sb.toString();
        } while (!usedEndToEndIds.add(id));
        return id;
    }

    private BigDecimal randomAmount() {
        // 10.00 – 999.99: sempre maior que o delta máximo da divergência, então o valor pago segue positivo.
        return BigDecimal.valueOf(1000 + random.nextInt(99000), 2).setScale(2, RoundingMode.UNNECESSARY);
    }

    private static int pct(int total, int percent) {
        return Math.round(total * percent / 100f);
    }
}
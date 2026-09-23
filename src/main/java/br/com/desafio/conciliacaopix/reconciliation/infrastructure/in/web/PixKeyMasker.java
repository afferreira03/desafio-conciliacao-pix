package br.com.desafio.conciliacaopix.reconciliation.infrastructure.in.web;

/**
 * Mascara a chave Pix (dado pessoal — LGPD) antes de expô-la pela API.
 * <ul>
 *   <li>E-mail: mantém os 3 primeiros caracteres do usuário e o domínio ({@code use***@email.com}).</li>
 *   <li>Demais (CPF/CNPJ, telefone, chave aleatória): mantém apenas os 4 últimos caracteres.</li>
 * </ul>
 */
public final class PixKeyMasker {

    private static final int VISIBLE_EMAIL_PREFIX = 3;
    private static final int VISIBLE_SUFFIX = 4;

    private PixKeyMasker() {
    }

    public static String mask(String pixKey) {
        if (pixKey == null || pixKey.isBlank()) {
            return pixKey;
        }

        int at = pixKey.indexOf('@');
        if (at > 0) {
            String user = pixKey.substring(0, at);
            String visible = user.substring(0, Math.min(VISIBLE_EMAIL_PREFIX, user.length()));
            return visible + "***" + pixKey.substring(at);
        }

        if (pixKey.length() <= VISIBLE_SUFFIX) {
            return "*".repeat(pixKey.length());
        }
        int hidden = pixKey.length() - VISIBLE_SUFFIX;
        return "*".repeat(hidden) + pixKey.substring(hidden);
    }
}

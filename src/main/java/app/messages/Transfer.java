package app.messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import utils.Crypto;

/**
 * A transfer of {@code amount} units from account {@code from} to account
 * {@code to}. This is the operation (or transaction) that is replicated.
 *
 * <p>
 * Each account has its own sequence of transfers: the transfer with sequence
 * number {@code seq} can only be executed after all transfers of the same
 * account with sequence numbers 0, 1, ..., {@code seq - 1}.
 *
 * <p>
 * The transfer is signed by {@code signer}, which must be one of the owners
 * of the account {@code from}. Replicas can therefore verify that a transfer
 * was really issued by an owner of the account, independently of who (client
 * or replica) delivers it to them.
 *
 * <p>
 * Records with arrays do not compare their contents by default, so we
 * override equals and hashCode.
 */
@BabelIPMessage(id = 100)
public record Transfer(String from, String to, long amount, long seq, String signer, byte[] signature)
        implements BabelMessage {

    /**
     * Creates a new transfer signed by the given owner of the account.
     */
    public static Transfer create(String from, String to, long amount, long seq, String signer,
            PrivateKey signerKey) throws GeneralSecurityException {
        byte[] signature = Crypto.sign(signedContent(from, to, amount, seq, signer), signerKey);
        return new Transfer(from, to, amount, seq, signer, signature);
    }

    /**
     * The bytes covered by the signature of the owner.
     */
    public byte[] signedContent() {
        return signedContent(from, to, amount, seq, signer);
    }

    /**
     * Identifier of the operation (hash of the signed content). Two transfers of
     * the same account with the same sequence number but a different id are
     * conflicting transfers.
     */
    public String id() {
        return Crypto.toHex(Crypto.digest(signedContent()));
    }

    private static byte[] signedContent(String from, String to, long amount, long seq, String signer) {
        // Length-prefixed UTF-8 strings: unambiguous, and (unlike writeUTF) never
        // fails, whatever the size of the strings sent by a (Byzantine) client
        try (var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes)) {
            writeString(out, from);
            writeString(out, to);
            out.writeLong(amount);
            out.writeLong(seq);
            writeString(out, signer);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(data.length);
        out.write(data);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Transfer other && from.equals(other.from) && to.equals(other.to)
                && amount == other.amount && seq == other.seq && signer.equals(other.signer)
                && Arrays.equals(signature, other.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to, amount, seq, signer) * 31 + Arrays.hashCode(signature);
    }

    @Override
    public String toString() {
        return "Transfer[" + from + "#" + seq + " -> " + to + ", amount=" + amount + ", signer=" + signer + "]";
    }
}

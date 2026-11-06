package blockchain;

import app.messages.Transfer;
import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import utils.Crypto;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

@BabelIPMessage(id = 300)
public record Block(
        String parentHash,
        long height,
        List<Transfer> transfers,
        long nonce
) implements BabelMessage {

    public static final int HASH_BYTES = 32;
    public static final int HASH_HEX_CHARACTERS = HASH_BYTES * 2;

    // Used only as the parent of the local genesis block.
    public static final String NO_PARENT_HASH = "0".repeat(HASH_HEX_CHARACTERS);

    private static final int FORMAT_VERSION = 1;

    private static final Block GENESIS = new Block(NO_PARENT_HASH, 0, List.of(), 0);

    public Block {
        Objects.requireNonNull(parentHash, "parentHash");
        Objects.requireNonNull(transfers, "transfers");

        // Deep copy avoiding mutability
        transfers = transfers.stream()
                .map(Block::copyTransfer).toList();
    }

    public static Block genesis() { return GENESIS; }

    public boolean isGenesis() { return equals(GENESIS); }

    // Avoid exposing signatures
    @Override
    public List<Transfer> transfers() {
        return transfers.stream()
                .map(Block::copyTransfer)
                .toList();
    }

    public byte[] hashBytes() { return Crypto.digest(canonicalBytes()); }

    public String hash() { return Crypto.toHex(hashBytes()); }

    public Block withNonce(long newNonce) { return new Block(parentHash, height, transfers, newNonce); }

    public boolean hasWellFormedParentHash() {
        if (parentHash.length() != HASH_HEX_CHARACTERS)
            return false;

        try {
            byte[] decoded = HexFormat.of().parseHex(parentHash);

            return decoded.length == HASH_BYTES && HexFormat.of().formatHex(decoded).equals(parentHash);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public boolean hasValidProofOfWork(int difficultyBits) {
        return hasLeadingZeroBits(hashBytes(), difficultyBits);
    }

    static boolean hasLeadingZeroBits(byte[] hashBytes, int difficultyBits) {
        Objects.requireNonNull(hashBytes, "hashBytes");

        if (difficultyBits < 0 || difficultyBits > hashBytes.length * Byte.SIZE)
            throw new IllegalArgumentException("Invalid difficulty: " + difficultyBits);

        int completeBytes = difficultyBits / Byte.SIZE;

        for (int i = 0; i < completeBytes; i++)
            if (hashBytes[i] != 0)
                return false;

        int remainingBits = difficultyBits % Byte.SIZE;
        if (remainingBits == 0)
            return true;

        // We must now check the remainder bits with a bit mask
        int mask = 0xff << (Byte.SIZE - remainingBits);
        return (Byte.toUnsignedInt(hashBytes[completeBytes]) & mask) == 0;
    }

    private byte[] canonicalBytes() {
        try(
                var bytes = new ByteArrayOutputStream();
                var output = new DataOutputStream(bytes)
        ) {
            output.writeInt(FORMAT_VERSION);
            writeString(output, parentHash);
            output.writeLong(height);
            output.writeInt(transfers.size());
            for (Transfer transfer : transfers) {
                writeBytes(output, transfer.signedContent());
                writeBytes(output, transfer.signature());
            }
            output.writeLong(nonce);
            output.flush();

            return bytes.toByteArray();
        } catch (IOException e) {
            // Thanks, Java >:)
            throw new UncheckedIOException(e);
        }
    }

    private static void writeString(DataOutputStream output, String value)
            throws IOException { writeBytes(output, value.getBytes(StandardCharsets.UTF_8)); }

    private static void writeBytes(DataOutputStream output, byte[] value)
            throws IOException { output.writeInt(value.length); output.write(value); }

    private static Transfer copyTransfer(Transfer transfer) {
        Objects.requireNonNull(transfer, "transfer");

        return new Transfer(
                transfer.from(),
                transfer.to(),
                transfer.amount(),
                transfer.seq(),
                transfer.signer(),
                transfer.signature().clone()
        );
    }

}

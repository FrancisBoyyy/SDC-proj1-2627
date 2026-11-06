package blockchain;

import java.util.Properties;

/**
 * Configuration shared by all replicas running the blockchain variant.
 */
public record BlockchainConfig(
    int difficultyBits,
    int maxTransactionsPerBlock,
    int confirmations,
    int maxOrphans,
    int maxPendingTransfers
) {

    public static final String DIFFICULTY_BITS_KEY = "blockchain_difficulty_bits";
    public static final String BLOCK_SIZE_KEY = "blockchain_block_size";
    public static final String CONFIRMATIONS_KEY = "blockchain_confirmations";
    public static final String MAX_ORPHANS_KEY = "blockchain_max_orphans";
    public static final String MAX_PENDING_TRANSFERS_KEY = "blockchain_max_pending_transfers";

    public static final int DEFAULT_DIFFICULTY_BITS = 16;
    public static final int DEFAULT_BLOCK_SIZE = 100;
    public static final int DEFAULT_CONFIRMATIONS = 2;
    public static final int DEFAULT_MAX_ORPHANS = 1_000;
    public static final int DEFAULT_MAX_PENDING_TRANSFERS = 10_000;

    // Sanitization
    public BlockchainConfig {
        if (difficultyBits < 0 || difficultyBits > 256)
            throw new IllegalArgumentException("Difficulty must be between 0 and 256 bits");
        if (maxTransactionsPerBlock <= 0)
            throw new IllegalArgumentException("Maximum transactions per block must be positive");
        if (confirmations < 0)
            throw new IllegalArgumentException("Confirmation depth must not be negative");
        if (maxOrphans < 0)
            throw new IllegalArgumentException("Maximum orphan count must not be negative");
        if (maxPendingTransfers <= 0)
            throw new IllegalArgumentException("Maximum pending transfer count must be positive");
    }

    public static BlockchainConfig from(Properties properties) {
        return new BlockchainConfig(
                readInt(properties, DIFFICULTY_BITS_KEY, DEFAULT_DIFFICULTY_BITS),
                readInt(properties, BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE),
                readInt(properties, CONFIRMATIONS_KEY, DEFAULT_CONFIRMATIONS),
                readInt(properties, MAX_ORPHANS_KEY, DEFAULT_MAX_ORPHANS),
                readInt(properties, MAX_PENDING_TRANSFERS_KEY, DEFAULT_MAX_PENDING_TRANSFERS)
        );
    }

    private static int readInt(Properties properties, String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null)
            return defaultValue;

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for property " + key + ": " + value, e);
        }
    }
}

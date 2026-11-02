package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;

import org.junit.jupiter.api.Test;

class BlockchainConfigTest {

    @Test
    void usesDefaultsWhenPropertiesAreMissing() {
        BlockchainConfig config = BlockchainConfig.from(new Properties());

        assertEquals(16, config.difficultyBits());
        assertEquals(100, config.maxTransactionsPerBlock());
        assertEquals(2, config.confirmations());
        assertEquals(1_000, config.maxOrphans());
        assertEquals(10_000, config.maxPendingTransfers());
    }

    @Test
    void readsConfiguredValues() {
        var properties = new Properties();
        properties.setProperty(BlockchainConfig.DIFFICULTY_BITS_KEY, "8");
        properties.setProperty(BlockchainConfig.BLOCK_SIZE_KEY, "25");
        properties.setProperty(BlockchainConfig.CONFIRMATIONS_KEY, "3");
        properties.setProperty(BlockchainConfig.MAX_ORPHANS_KEY, "50");
        properties.setProperty(BlockchainConfig.MAX_PENDING_TRANSFERS_KEY, "500");

        BlockchainConfig config = BlockchainConfig.from(properties);

        assertEquals(8, config.difficultyBits());
        assertEquals(25, config.maxTransactionsPerBlock());
        assertEquals(3, config.confirmations());
        assertEquals(50, config.maxOrphans());
        assertEquals(500, config.maxPendingTransfers());
    }

    @Test
    void rejectsMalformedInteger() {
        var properties = new Properties();
        properties.setProperty(BlockchainConfig.DIFFICULTY_BITS_KEY, "sixteen");

        assertThrows(
                IllegalArgumentException.class,
                () -> BlockchainConfig.from(properties));
    }

    @Test
    void rejectsValuesOutsideAllowedRanges() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockchainConfig(-1, 1, 0, 0, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockchainConfig(257, 1, 0, 0, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockchainConfig(0, 0, 0, 0, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockchainConfig(0, 1, -1, 0, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockchainConfig(0, 1, 0, -1, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockchainConfig(0, 1, 0, 0, 0));
    }
}
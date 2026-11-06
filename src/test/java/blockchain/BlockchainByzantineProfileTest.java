package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class BlockchainByzantineProfileTest {

    @Test
    void parsesKnownProfilesCaseInsensitively() {
        assertEquals(
                BlockchainByzantineProfile.NONE,
                BlockchainByzantineProfile.fromProperty("none"));
        assertEquals(
                BlockchainByzantineProfile.WITHHOLD_BLOCKS,
                BlockchainByzantineProfile.fromProperty(" WITHHOLD_BLOCKS "));
        assertEquals(
                BlockchainByzantineProfile.BROADCAST_INVALID_BLOCK,
                BlockchainByzantineProfile.fromProperty("broadcast_invalid_block"));
        assertEquals(
                BlockchainByzantineProfile.SELECTIVE_SEND,
                BlockchainByzantineProfile.fromProperty(" SELECTIVE_SEND "));
    }

    @Test
    void rejectsUnknownProfile() {
        assertThrows(
                IllegalArgumentException.class,
                () -> BlockchainByzantineProfile.fromProperty("equivocate"));
    }
}

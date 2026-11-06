package blockchain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

class BlockTest {

    @Test
    void genesisIsDeterministic() {
        assertEquals(Block.genesis().hash(), Block.genesis().hash());
        assertEquals(0, Block.genesis().height());
        assertTrue(Block.genesis().isGenesis());
    }

    @Test
    void nonceChangesHash() {
        Block block = new Block(
                Block.genesis().hash(),
                1,
                List.of(),
                0);

        assertNotEquals(block.hash(), block.withNonce(1).hash());
    }

    @Test
    void countsLeadingZeroBits() {
        assertTrue(Block.hasLeadingZeroBits(
                new byte[] {0, 0x0f}, 12));

        assertFalse(Block.hasLeadingZeroBits(
                new byte[] {0, 0x0f}, 13));
    }

    @Test
    void difficultyZeroAlwaysPasses() {
        Block block = new Block(
                Block.genesis().hash(),
                1,
                List.of(),
                42);

        assertTrue(block.hasValidProofOfWork(0));
    }

}

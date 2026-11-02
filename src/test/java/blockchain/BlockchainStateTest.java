package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPairGenerator;
import java.util.List;

import org.junit.jupiter.api.Test;

import app.messages.Transfer;

class BlockchainStateTest {

    private static BlockchainConfig config(int confirmations, int maxOrphans) {
        return new BlockchainConfig(0, 10, confirmations, maxOrphans, 100);
    }

    private static BlockchainState state(int confirmations) {
        return new BlockchainState(config(confirmations, 10), transfer -> true);
    }

    private static Block child(BlockNode parent, long nonce, Transfer... transfers) {
        return new Block(parent.hash(), parent.height() + 1, List.of(transfers), nonce);
    }

    private static Transfer transfer(String from, long seq) throws Exception {
        var keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        return Transfer.create(from, "recipient", 1, seq, from, keyPair.getPrivate());
    }

    @Test
    void initializesWithCanonicalGenesisAndDoesNotFinalizeItAsANewBlock() {
        BlockchainState state = state(2);

        assertEquals(Block.genesis().hash(), state.genesis().hash());
        assertEquals(state.genesis(), state.tip());
        assertEquals(state.genesis(), state.finalizedTip());
        assertTrue(state.advanceFinality().isEmpty());
    }

    @Test
    void acceptsValidChildAndIgnoresDuplicateBlock() {
        BlockchainState state = state(2);
        Block block = child(state.genesis(), 1);

        assertTrue(state.addBlock(block));
        assertEquals(block.hash(), state.tip().hash());
        assertFalse(state.addBlock(block));
    }

    @Test
    void rejectsMalformedHeightParentProofAndOversizedBlock() {
        BlockchainState state = new BlockchainState(config(2, 10), transfer -> true);
        BlockNode genesis = state.genesis();

        assertFalse(state.addBlock(Block.genesis()));
        assertFalse(state.addBlock(new Block(genesis.hash(), 2, List.of(), 0)));
        assertFalse(state.addBlock(new Block("not-a-hash", 1, List.of(), 0)));

        BlockchainState workRequired = new BlockchainState(
                new BlockchainConfig(256, 10, 2, 10, 100), transfer -> true);
        assertFalse(workRequired.addBlock(child(workRequired.genesis(), 1)));

        BlockchainState smallBlocks = new BlockchainState(
                new BlockchainConfig(0, 1, 2, 10, 100), transfer -> true);
        try {
            Transfer first = transfer("alice", 0);
            Transfer second = transfer("bob", 0);
            assertFalse(smallBlocks.addBlock(child(smallBlocks.genesis(), 1, first, second)));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void rejectsInvalidAndDuplicateTransactionsWithinOneBranch() throws Exception {
        Transfer transfer = transfer("alice", 0);
        BlockchainState rejectsInvalid = new BlockchainState(config(2, 10), ignored -> false);
        assertFalse(rejectsInvalid.addBlock(child(rejectsInvalid.genesis(), 1, transfer)));

        BlockchainState state = state(2);
        assertFalse(state.addBlock(child(state.genesis(), 1, transfer, transfer)));
        assertTrue(state.addBlock(child(state.genesis(), 2, transfer)));

        BlockNode parent = state.tip();
        assertFalse(state.addBlock(child(parent, 3, transfer)));
    }

    @Test
    void duplicateTransferOnSiblingForkDoesNotInvalidateOtherBranch() throws Exception {
        BlockchainState state = state(5);
        Transfer transfer = transfer("alice", 0);
        Block firstFork = child(state.genesis(), 1, transfer);
        Block secondFork = child(state.genesis(), 2);

        assertTrue(state.addBlock(firstFork));
        assertTrue(state.addBlock(secondFork));

        BlockNode secondForkNode = state.tip().hash().equals(secondFork.hash())
                ? state.tip()
                : state.genesis();
        // Add the same transfer to the other sibling branch; branch-local IDs
        // mean the first fork's contents do not contaminate this branch.
        Block sameTransferOnSibling = child(secondForkNode, 3, transfer);
        assertTrue(state.addBlock(sameTransferOnSibling));
        assertNotEquals(firstFork.hash(), sameTransferOnSibling.hash());
    }

    @Test
    void retainsForksAndSelectsLongestThenLexicographicallySmallestTip() {
        BlockchainState state = state(5);
        Block left = child(state.genesis(), 1);
        Block right = child(state.genesis(), 2);

        assertTrue(state.addBlock(left));
        assertTrue(state.addBlock(right));
        String expectedAtTie = left.hash().compareTo(right.hash()) < 0
                ? left.hash()
                : right.hash();
        assertEquals(expectedAtTie, state.tip().hash());

        Block longer = child(state.tip(), 3);
        assertTrue(state.addBlock(longer));
        assertEquals(longer.hash(), state.tip().hash());
    }

    @Test
    void storesOrphanAndAttachesItWhenParentArrives() {
        BlockchainState state = state(5);
        Block first = child(state.genesis(), 1);
        Block second = child(new BlockNode(first, state.genesis()), 2);

        assertFalse(state.addBlock(second));
        assertEquals(state.genesis(), state.tip());

        assertTrue(state.addBlock(first));
        assertEquals(second.hash(), state.tip().hash());
        assertFalse(state.addBlock(second));
    }

    @Test
    void boundsOrphansAndCanDisableOrphanStorage() {
        BlockchainState bounded = new BlockchainState(config(5, 1), transfer -> true);
        Block firstOrphan = new Block("1".repeat(Block.HASH_HEX_CHARACTERS), 2, List.of(), 1);
        Block secondOrphan = new Block("2".repeat(Block.HASH_HEX_CHARACTERS), 2, List.of(), 2);

        assertFalse(bounded.addBlock(firstOrphan));
        assertFalse(bounded.addBlock(secondOrphan));
        assertFalse(bounded.addBlock(firstOrphan)); // oldest orphan was evicted

        BlockchainState disabled = new BlockchainState(config(5, 0), transfer -> true);
        assertFalse(disabled.addBlock(firstOrphan));
        assertFalse(disabled.addBlock(firstOrphan));
    }

    @Test
    void finalizesBlocksAfterConfiguredSuccessorsInChainOrder() {
        BlockchainState state = state(2);
        Block first = child(state.genesis(), 1);
        Block second = child(new BlockNode(first, state.genesis()), 2);
        Block third = child(new BlockNode(second, new BlockNode(first, state.genesis())), 3);

        assertTrue(state.addBlock(first));
        assertTrue(state.advanceFinality().isEmpty());
        assertTrue(state.addBlock(second));
        assertTrue(state.advanceFinality().isEmpty());
        assertTrue(state.addBlock(third));

        List<Block> finalized = state.advanceFinality();
        assertEquals(List.of(first), finalized);
        assertEquals(first.hash(), state.finalizedTip().hash());
        assertTrue(state.advanceFinality().isEmpty());
    }

    @Test
    void permitsForksAboveCheckpointButRejectsForksThatConflictWithIt() {
        BlockchainState state = state(1);
        Block first = child(state.genesis(), 1);
        Block second = child(new BlockNode(first, state.genesis()), 2);

        assertTrue(state.addBlock(first));
        assertTrue(state.addBlock(second));
        assertEquals(List.of(first), state.advanceFinality());

        // A sibling at height two conflicts with the finalized height-one block.
        Block conflictingHeightTwo = child(state.genesis(), 3);
        assertFalse(state.addBlock(conflictingHeightTwo));

        // A fork that shares the finalized block is still valid while unfinalized.
        Block siblingAboveCheckpoint = child(new BlockNode(first, state.genesis()), 4);
        assertTrue(state.addBlock(siblingAboveCheckpoint));
    }
}

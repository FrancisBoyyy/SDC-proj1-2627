package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import app.ledger.Ledger;
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
        assertTrue(state.advanceStableBlocks().isEmpty());
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
    void rejectsInvalidProofWithoutChangingChainAndAcceptsLaterValidBlock() {
        BlockchainConfig config = new BlockchainConfig(1, 10, 0, 10, 100);
        BlockchainState state = new BlockchainState(config, transfer -> true);
        BlockNode genesis = state.genesis();

        Block invalidProof = childWithProof(genesis, 1, false);

        assertFalse(state.addBlock(invalidProof));
        assertEquals(genesis.hash(), state.tip().hash());
        assertTrue(state.advanceStableBlocks().isEmpty());

        Block validBlock = childWithProof(genesis, 1, true);

        assertTrue(state.addBlock(validBlock));
        assertEquals(List.of(validBlock), state.advanceStableBlocks());
    }

    @Test
    void rejectsMalformedParentWrongHeightAndOversizedBlockWithoutPoisoningState()
            throws Exception {
        BlockchainConfig config = new BlockchainConfig(0, 1, 0, 10, 100);
        BlockchainState state = new BlockchainState(config, transfer -> true);
        BlockNode genesis = state.genesis();

        Block malformedParent = new Block("not-a-hash", 1, List.of(), 0);
        Block wrongHeight = new Block(genesis.hash(), 2, List.of(), 0);

        Transfer first = transfer("alice", 0);
        Transfer second = transfer("bob", 0);
        Block oversized = child(genesis, 3, first, second);

        assertFalse(state.addBlock(malformedParent));
        assertFalse(state.addBlock(wrongHeight));
        assertFalse(state.addBlock(oversized));

        assertEquals(genesis.hash(), state.tip().hash());
        assertTrue(state.advanceStableBlocks().isEmpty());

        Block validBlock = child(genesis, 4);

        assertTrue(state.addBlock(validBlock));
        assertEquals(List.of(validBlock), state.advanceStableBlocks());
    }

    @Test
    void rejectsInvalidAndDuplicateTransactionsWithinOneBranch() throws Exception {
        Transfer transfer = transfer("alice", 0);
        BlockchainState rejectsInvalid = new BlockchainState(config(2, 10), ignored -> false);
        assertFalse(rejectsInvalid.addBlock(child(rejectsInvalid.genesis(), 1, transfer)));
        assertEquals(rejectsInvalid.genesis().hash(), rejectsInvalid.tip().hash());
        assertTrue(rejectsInvalid.advanceStableBlocks().isEmpty());

        BlockchainState state = state(2);
        assertFalse(state.addBlock(child(state.genesis(), 1, transfer, transfer)));
        assertEquals(state.genesis().hash(), state.tip().hash());
        assertTrue(state.advanceStableBlocks().isEmpty());

        assertTrue(state.addBlock(child(state.genesis(), 2, transfer)));

        BlockNode parent = state.tip();
        assertFalse(state.addBlock(child(parent, 3, transfer)));
        assertEquals(parent.hash(), state.tip().hash());
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
    void marksBlocksStableAfterConfiguredSuccessorsInChainOrder() {
        BlockchainState state = state(2);
        Block first = child(state.genesis(), 1);
        Block second = child(new BlockNode(first, state.genesis()), 2);
        Block third = child(new BlockNode(second, new BlockNode(first, state.genesis())), 3);

        assertTrue(state.addBlock(first));
        assertTrue(state.advanceStableBlocks().isEmpty());
        assertTrue(state.addBlock(second));
        assertTrue(state.advanceStableBlocks().isEmpty());
        assertTrue(state.addBlock(third));

        List<Block> stable = state.advanceStableBlocks();
        assertEquals(List.of(first), stable);
        assertTrue(state.advanceStableBlocks().isEmpty());
    }

    @Test
    void adoptsLongerForkEvenWhenItConflictsWithPreviouslyStableBlock() throws Exception {
        BlockchainState state = state(1);
        Transfer previousTransfer = transfer("alice", 0);
        Transfer competingTransfer = transfer("bob", 0);
        Block first = child(state.genesis(), 1, previousTransfer);
        Block second = child(new BlockNode(first, state.genesis()), 2);

        assertTrue(state.addBlock(first));
        assertTrue(state.addBlock(second));
        assertEquals(List.of(first), state.advanceStableBlocks());

        Block rivalFirst = child(state.genesis(), 3, competingTransfer);
        Block rivalSecond = child(new BlockNode(rivalFirst, state.genesis()), 4);
        Block rivalThird = child(
                new BlockNode(rivalSecond, new BlockNode(rivalFirst, state.genesis())), 5);

        // The rival branch is delivered backwards; its blocks are retained as orphans.
        assertFalse(state.addBlock(rivalThird));
        assertFalse(state.addBlock(rivalSecond));
        assertTrue(state.addBlock(rivalFirst));
        assertEquals(rivalThird.hash(), state.tip().hash());

        // The selected chain now stabilizes the competing block. The previous
        // transfer remains handed off; the application layer cannot roll it back.
        assertEquals(List.of(rivalFirst, rivalSecond), state.advanceStableBlocks());
        assertTrue(state.advanceStableBlocks().isEmpty());
    }

    @Test
    void rejectsInvalidSignatureWithoutPoisoningLaterValidTransfer() throws Exception {
        var keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();

        Ledger ledger = new Ledger(
                Map.of(
                        "alice", Set.of("alice"),
                        "bob", Set.<String>of()
                ),
                Map.of("alice", keyPair.getPublic()),
                100
        );

        Transfer validTransfer = Transfer.create(
                "alice", "bob", 5, 0, "alice", keyPair.getPrivate());

        byte[] tamperedSignature = validTransfer.signature().clone();
        tamperedSignature[0] ^= 1;

        Transfer invalidSignature = new Transfer(
                validTransfer.from(),
                validTransfer.to(),
                validTransfer.amount(),
                validTransfer.seq(),
                validTransfer.signer(),
                tamperedSignature
        );

        assertEquals(validTransfer.id(), invalidSignature.id());
        assertFalse(ledger.isValid(invalidSignature));

        BlockchainConfig config = new BlockchainConfig(0, 10, 0, 10, 100);
        BlockchainState state = new BlockchainState(config, ledger::isValid);
        BlockNode genesis = state.genesis();

        Block invalidBlock = child(genesis, 1, invalidSignature);

        assertFalse(state.addBlock(invalidBlock));
        assertEquals(0, ledger.size());
        assertEquals(100, ledger.balance("alice"));
        assertTrue(state.advanceStableBlocks().isEmpty());

        Block validBlock = child(genesis, 2, validTransfer);

        assertTrue(state.addBlock(validBlock));
        List<Block> stable = state.advanceStableBlocks();
        assertEquals(List.of(validBlock), stable);

        for (Block block : stable) {
            for (Transfer transfer : block.transfers()) {
                ledger.execute(transfer);
            }
        }

        assertEquals(1, ledger.size());
        assertEquals(95, ledger.balance("alice"));
        assertEquals(105, ledger.balance("bob"));
    }

    private static Block childWithProof(
            BlockNode parent,
            int difficulty,
            boolean shouldBeValid,
            Transfer... transfers
    ) {
        for (long nonce = 0; nonce < 1_024; nonce++) {
            Block candidate = child(parent, nonce, transfers);
            if (candidate.hasValidProofOfWork(difficulty) == shouldBeValid) {
                return candidate;
            }
        }

        throw new AssertionError("Could not find block with requested proof-of-work result");
    }
}

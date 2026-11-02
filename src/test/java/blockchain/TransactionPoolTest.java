package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPairGenerator;
import java.util.List;

import org.junit.jupiter.api.Test;

import app.messages.Transfer;

class TransactionPoolTest {

    private static Transfer transfer(String from, String to, long amount, long seq) throws Exception {
        var keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        return Transfer.create(from, to, amount, seq, from, keyPair.getPrivate());
    }

    private static BlockNode genesis() {
        return new BlockNode(Block.genesis(), null);
    }

    private static BlockNode child(BlockNode parent, long nonce, Transfer... transfers) {
        return new BlockNode(
                new Block(parent.hash(), parent.height() + 1, List.of(transfers), nonce),
                parent,
                parent.transferIds());
    }

    @Test
    void rejectsInvalidTransfersAndDeduplicatesByTransferId() throws Exception {
        Transfer valid = transfer("alice", "bob", 5, 0);
        TransactionPool pool = new TransactionPool(10, candidate -> candidate.amount() > 0);
        TransactionPool rejectingPool = new TransactionPool(10, candidate -> false);

        assertFalse(rejectingPool.add(valid));
        assertEquals(0, rejectingPool.size());
        assertTrue(pool.add(valid));
        assertFalse(pool.add(valid));
        assertEquals(1, pool.size());
        assertTrue(pool.contains(valid.id()));
    }

    @Test
    void enforcesCapacityAndRejectsInvalidCapacity() throws Exception {
        TransactionPool pool = new TransactionPool(1, candidate -> true);
        Transfer first = transfer("alice", "bob", 5, 0);
        Transfer second = transfer("carol", "dave", 7, 0);

        assertTrue(pool.add(first));
        assertFalse(pool.add(second));
        assertEquals(1, pool.size());
        assertThrows(IllegalArgumentException.class,
                () -> new TransactionPool(0, candidate -> true));
        assertThrows(IllegalArgumentException.class,
                () -> new TransactionPool(-1, candidate -> true));
    }

    @Test
    void returnsCandidatesInDeterministicOrderAndHonorsLimit() throws Exception {
        Transfer zoe = transfer("zoe", "sink", 3, 0);
        Transfer bob = transfer("bob", "sink", 2, 0);
        Transfer alice = transfer("alice", "sink", 1, 0);
        TransactionPool pool = new TransactionPool(10, candidate -> true);
        pool.add(zoe);
        pool.add(alice);
        pool.add(bob);

        List<Transfer> candidate = pool.candidate(genesis(), 2);

        assertEquals(List.of(alice.id(), bob.id()),
                candidate.stream().map(Transfer::id).toList());
        assertThrows(IllegalArgumentException.class,
                () -> pool.candidate(genesis(), 0));
    }

    @Test
    void excludesTransfersAlreadyPresentOnTheSelectedBranchOnly() throws Exception {
        Transfer includedOnLeft = transfer("alice", "bob", 5, 0);
        TransactionPool pool = new TransactionPool(10, candidate -> true);
        pool.add(includedOnLeft);

        BlockNode root = genesis();
        BlockNode left = child(root, 1, includedOnLeft);
        BlockNode right = child(root, 2);

        assertTrue(pool.candidate(left, 10).isEmpty());
        assertEquals(List.of(includedOnLeft.id()),
                pool.candidate(right, 10).stream().map(Transfer::id).toList());
        assertTrue(pool.contains(includedOnLeft.id()));
    }

    @Test
    void selectsOnlyOneTransferForAnAccountSequence() throws Exception {
        Transfer conflictA = transfer("alice", "bob", 5, 0);
        Transfer conflictB = transfer("alice", "carol", 6, 0);
        Transfer nextSequence = transfer("alice", "dave", 1, 1);
        TransactionPool pool = new TransactionPool(10, candidate -> true);
        pool.add(conflictB);
        pool.add(nextSequence);
        pool.add(conflictA);

        List<Transfer> candidate = pool.candidate(genesis(), 10);

        assertEquals(2, candidate.size());
        assertEquals(1, candidate.stream()
                .filter(transfer -> transfer.from().equals("alice") && transfer.seq() == 0)
                .count());
        assertTrue(candidate.stream().anyMatch(transfer -> transfer.id().equals(nextSequence.id())));
    }

    @Test
    void removesTransfersOnlyWhenTheirBlockIsFinalized() throws Exception {
        Transfer transfer = transfer("alice", "bob", 5, 0);
        TransactionPool pool = new TransactionPool(10, candidate -> true);
        pool.add(transfer);

        BlockNode unfinalizedBlock = child(genesis(), 1, transfer);
        assertTrue(pool.contains(transfer.id()));
        assertTrue(pool.candidate(genesis(), 10).stream()
                .anyMatch(candidate -> candidate.id().equals(transfer.id())));

        pool.removeFinalized(unfinalizedBlock.block());

        assertFalse(pool.contains(transfer.id()));
        assertEquals(0, pool.size());
    }
}

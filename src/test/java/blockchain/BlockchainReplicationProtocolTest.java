package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import app.ledger.Ledger;
import app.messages.Transfer;
import org.junit.jupiter.api.Test;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import replication.events.ExecuteTransaction;

import java.security.KeyPairGenerator;
import java.util.Map;
import java.util.Set;

class BlockchainReplicationProtocolTest {

    private static BlockchainConfig config(int confirmations) {
        return new BlockchainConfig(0, 10, confirmations, 10, 10);
    }

    private static Block child(Block parent, List<Transfer> transfers) {
        return new Block(parent.hash(), parent.height() + 1, transfers, 0);
    }

    @Test
    void executesTransfersOnlyAfterTheirBlockHasEnoughConfirmations()
            throws Exception {
        BlockingQueue<ExecuteTransaction> executed = new LinkedBlockingQueue<>();
        Babel runtime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, executed::add)
                .build()
                .init();
        BlockchainReplicationProtocol protocol = new BlockchainReplicationProtocol(
                config(1), ignored -> true);

        try {
            protocol.init(runtime, null);
            Transfer transfer = new Transfer("from", "to", 1, 0, "signer", new byte[0]);
            Block blockWithTransfer = child(Block.genesis(), List.of(transfer));
            Block confirmationBlock = child(blockWithTransfer, List.of());

            protocol.uponDeliverBlock(blockWithTransfer, runtime.getMyself());
            assertTrue(executed.isEmpty(), "Transfer executed before confirmation");

            protocol.uponDeliverBlock(confirmationBlock, runtime.getMyself());

            ExecuteTransaction event = executed.poll(2, TimeUnit.SECONDS);
            assertTrue(event != null, "Finalized transfer was not delivered to the application");
            assertEquals(transfer, event.transfer());
        } finally {
            protocol.stop();
            runtime.shutdown();
        }
    }

    @Test
    void repeatedBlockDeliveryDoesNotExecuteTransferTwice() throws Exception {
        BlockingQueue<ExecuteTransaction> executed = new LinkedBlockingQueue<>();
        Babel runtime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, executed::add)
                .build()
                .init();
        BlockchainReplicationProtocol protocol = new BlockchainReplicationProtocol(
                config(0), ignored -> true);

        try {
            protocol.init(runtime, null);
            Transfer transfer = new Transfer("from", "to", 1, 0, "signer", new byte[0]);
            Block block = child(Block.genesis(), List.of(transfer));

            protocol.uponDeliverBlock(block, runtime.getMyself());
            ExecuteTransaction event = executed.poll(2, TimeUnit.SECONDS);
            assertTrue(event != null, "Immediately finalized transfer was not delivered");
            assertEquals(transfer, event.transfer());

            protocol.uponDeliverBlock(block, runtime.getMyself());
            assertTrue(executed.poll(200, TimeUnit.MILLISECONDS) == null,
                    "Repeated block delivery executed the transfer again");
        } finally {
            protocol.stop();
            runtime.shutdown();
        }
    }

    @Test
    void rejectsMalformedParentBlockAndStillAcceptsLaterValidBlock()
            throws Exception {
        BlockingQueue<ExecuteTransaction> executed = new LinkedBlockingQueue<>();
        Babel runtime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, executed::add)
                .build()
                .init();
        BlockchainReplicationProtocol protocol = new BlockchainReplicationProtocol(
                config(0), ignored -> true, false);

        try {
            protocol.init(runtime, null);

            Transfer transfer = new Transfer(
                    "from", "to", 1, 0, "signer", new byte[0]);
            Block invalidBlock = new Block(
                    "invalid-parent-hash", 1, List.of(transfer), 0);

            protocol.uponDeliverBlock(invalidBlock, runtime.getMyself());
            assertTrue(executed.isEmpty(), "Malformed-parent block was executed");

            Block validBlock = child(Block.genesis(), List.of(transfer));
            protocol.uponDeliverBlock(validBlock, runtime.getMyself());

            ExecuteTransaction event = executed.poll(2, TimeUnit.SECONDS);
            assertTrue(event != null, "Valid block after rejection was not executed");
            assertEquals(transfer, event.transfer());
        } finally {
            protocol.stop();
            runtime.shutdown();
        }
    }

    @Test
    void attachesOutOfOrderBlockAndExecutesItsTransferOnlyOnce()
            throws Exception {
        BlockingQueue<ExecuteTransaction> executed = new LinkedBlockingQueue<>();
        Babel runtime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, executed::add)
                .build()
                .init();
        BlockchainReplicationProtocol protocol = new BlockchainReplicationProtocol(
                config(0), ignored -> true, false);

        try {
            protocol.init(runtime, null);

            Transfer transfer = new Transfer(
                    "from", "to", 1, 0, "signer", new byte[0]);
            Block parent = child(Block.genesis(), List.of());
            Block child = child(parent, List.of(transfer));

            protocol.uponDeliverBlock(child, runtime.getMyself());
            protocol.uponDeliverBlock(child, runtime.getMyself());

            assertTrue(
                    executed.isEmpty(),
                    "Orphan transfer executed before its parent arrived");

            protocol.uponDeliverBlock(parent, runtime.getMyself());

            ExecuteTransaction event = executed.poll(2, TimeUnit.SECONDS);
            assertTrue(event != null, "Attached orphan transfer was not executed");
            assertEquals(transfer, event.transfer());

            protocol.uponDeliverBlock(child, runtime.getMyself());
            protocol.uponDeliverBlock(parent, runtime.getMyself());

            assertTrue(
                    executed.poll(200, TimeUnit.MILLISECONDS) == null,
                    "Duplicate delivery executed the transfer again");
        } finally {
            protocol.stop();
            runtime.shutdown();
        }
    }

    @Test
    void replicasSelectSameForkRegardlessOfDeliveryOrder()
            throws Exception {
        BlockingQueue<ExecuteTransaction> executedFirst = new LinkedBlockingQueue<>();
        BlockingQueue<ExecuteTransaction> executedSecond = new LinkedBlockingQueue<>();

        Babel firstRuntime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, executedFirst::add)
                .build()
                .init();
        Babel secondRuntime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, executedSecond::add)
                .build()
                .init();

        BlockchainReplicationProtocol firstProtocol =
                new BlockchainReplicationProtocol(config(1), ignored -> true, false);
        BlockchainReplicationProtocol secondProtocol =
                new BlockchainReplicationProtocol(config(1), ignored -> true, false);

        try {
            firstProtocol.init(firstRuntime, null);
            secondProtocol.init(secondRuntime, null);

            Transfer transferA =
                    new Transfer("alice", "recipient-a", 1, 0, "alice", new byte[0]);
            Transfer transferB =
                    new Transfer("bob", "recipient-b", 1, 0, "bob", new byte[0]);

            Block forkA = child(Block.genesis(), List.of(transferA));
            Block forkB = child(Block.genesis(), List.of(transferB));

            Block winner = forkA.hash().compareTo(forkB.hash()) < 0 ? forkA : forkB;
            Transfer winningTransfer = winner.equals(forkA) ? transferA : transferB;
            Block extension = child(winner, List.of());

            // Same blocks, different delivery orders.
            firstProtocol.uponDeliverBlock(forkA, firstRuntime.getMyself());
            firstProtocol.uponDeliverBlock(forkB, firstRuntime.getMyself());

            secondProtocol.uponDeliverBlock(forkB, secondRuntime.getMyself());
            secondProtocol.uponDeliverBlock(forkA, secondRuntime.getMyself());

            assertTrue(executedFirst.isEmpty(), "Fork transfer finalized too early");
            assertTrue(executedSecond.isEmpty(), "Fork transfer finalized too early");

            firstProtocol.uponDeliverBlock(extension, firstRuntime.getMyself());
            secondProtocol.uponDeliverBlock(extension, secondRuntime.getMyself());

            ExecuteTransaction firstExecution =
                    executedFirst.poll(2, TimeUnit.SECONDS);
            ExecuteTransaction secondExecution =
                    executedSecond.poll(2, TimeUnit.SECONDS);

            assertTrue(firstExecution != null, "First replica did not finalize the winner");
            assertTrue(secondExecution != null, "Second replica did not finalize the winner");
            assertEquals(winningTransfer, firstExecution.transfer());
            assertEquals(winningTransfer, secondExecution.transfer());

            assertTrue(
                    executedFirst.poll(200, TimeUnit.MILLISECONDS) == null,
                    "First replica executed a losing-fork transfer");
            assertTrue(
                    executedSecond.poll(200, TimeUnit.MILLISECONDS) == null,
                    "Second replica executed a losing-fork transfer");
        } finally {
            firstProtocol.stop();
            secondProtocol.stop();
            firstRuntime.shutdown();
            secondRuntime.shutdown();
        }
    }

    @Test
    void adoptsDeeperForkAfterStableTransferWithoutRollingBackApplicationEffect()
            throws Exception {
        BlockingQueue<ExecuteTransaction> executed = new LinkedBlockingQueue<>();
        Babel runtime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, executed::add)
                .build()
                .init();

        BlockchainReplicationProtocol protocol =
                new BlockchainReplicationProtocol(config(1), ignored -> true, false);

        try {
            protocol.init(runtime, null);

            Transfer previouslyStableTransfer =
                    new Transfer("alice", "recipient-a", 1, 0, "alice", new byte[0]);
            Transfer competingTransfer =
                    new Transfer("bob", "recipient-b", 1, 0, "bob", new byte[0]);

            Block previousFirst =
                    child(Block.genesis(), List.of(previouslyStableTransfer));
            Block previousSecond = child(previousFirst, List.of());

            protocol.uponDeliverBlock(previousFirst, runtime.getMyself());
            assertTrue(executed.isEmpty(), "Transfer was handed off before k confirmations");
            protocol.uponDeliverBlock(previousSecond, runtime.getMyself());

            ExecuteTransaction previousExecution =
                    executed.poll(2, TimeUnit.SECONDS);
            assertTrue(previousExecution != null, "Expected stable transfer was not handed off");
            assertEquals(previouslyStableTransfer, previousExecution.transfer());

            // Deliver the longer competing branch backwards so its blocks are
            // retained as orphans, then attached when its first block arrives.
            Block rivalFirst = child(Block.genesis(), List.of(competingTransfer));
            Block rivalSecond = child(rivalFirst, List.of());
            Block rivalThird = child(rivalSecond, List.of());

            protocol.uponDeliverBlock(rivalThird, runtime.getMyself());
            protocol.uponDeliverBlock(rivalSecond, runtime.getMyself());
            protocol.uponDeliverBlock(rivalFirst, runtime.getMyself());

            ExecuteTransaction competingExecution =
                    executed.poll(2, TimeUnit.SECONDS);
            assertTrue(
                    competingExecution != null,
                    "Longer valid fork was not adopted after the earlier transfer was handed off");
            assertEquals(competingTransfer, competingExecution.transfer());
            assertTrue(
                    executed.poll(200, TimeUnit.MILLISECONDS) == null,
                    "Previously handed-off transfer was sent to the application twice");
        } finally {
            protocol.stop();
            runtime.shutdown();
        }
    }

    @Test
    void deepReorganizationCannotUndoConflictingTransferAlreadyExecutedByLedger()
            throws Exception {
        var keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        Map<String, Set<String>> owners = Map.of(
                "alice", Set.of("alice"),
                "bob", Set.of(),
                "carol", Set.of());
        Map<String, java.security.PublicKey> keys = Map.of("alice", keyPair.getPublic());
        Ledger firstLedger = new Ledger(owners, keys, 100);
        Ledger secondLedger = new Ledger(owners, keys, 100);
        BlockingQueue<ExecuteTransaction> firstExecutions = new LinkedBlockingQueue<>();
        BlockingQueue<ExecuteTransaction> secondExecutions = new LinkedBlockingQueue<>();

        Babel firstRuntime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, event -> {
                    firstLedger.execute(event.transfer());
                    firstExecutions.add(event);
                })
                .build()
                .init();
        Babel secondRuntime = Babel.builder()
                .onDeliver(ExecuteTransaction.class, event -> {
                    secondLedger.execute(event.transfer());
                    secondExecutions.add(event);
                })
                .build()
                .init();
        BlockchainReplicationProtocol firstProtocol =
                new BlockchainReplicationProtocol(config(1), firstLedger::isValid, false);
        BlockchainReplicationProtocol secondProtocol =
                new BlockchainReplicationProtocol(config(1), secondLedger::isValid, false);

        try {
            firstProtocol.init(firstRuntime, null);
            secondProtocol.init(secondRuntime, null);

            Transfer oldBranchTransfer = Transfer.create(
                    "alice", "bob", 10, 0, "alice", keyPair.getPrivate());
            Transfer longerBranchTransfer = Transfer.create(
                    "alice", "carol", 10, 0, "alice", keyPair.getPrivate());
            Block oldFirst = child(Block.genesis(), List.of(oldBranchTransfer));
            Block oldSecond = child(oldFirst, List.of());
            Block newFirst = child(Block.genesis(), List.of(longerBranchTransfer));
            Block newSecond = child(newFirst, List.of());
            Block newThird = child(newSecond, List.of());

            // Replica one sees and executes the old branch before the reorganization.
            firstProtocol.uponDeliverBlock(oldFirst, firstRuntime.getMyself());
            firstProtocol.uponDeliverBlock(oldSecond, firstRuntime.getMyself());
            assertEquals(oldBranchTransfer,
                    firstExecutions.poll(2, TimeUnit.SECONDS).transfer());

            // Replica two sees only the competing longer branch before stability.
            secondProtocol.uponDeliverBlock(newFirst, secondRuntime.getMyself());
            secondProtocol.uponDeliverBlock(newSecond, secondRuntime.getMyself());
            secondProtocol.uponDeliverBlock(newThird, secondRuntime.getMyself());
            assertEquals(longerBranchTransfer,
                    secondExecutions.poll(2, TimeUnit.SECONDS).transfer());

            // Both replicas eventually learn the same blocks. The longer branch wins
            // locally, but the ledger at replica one cannot reverse its earlier spend.
            firstProtocol.uponDeliverBlock(newThird, firstRuntime.getMyself());
            firstProtocol.uponDeliverBlock(newSecond, firstRuntime.getMyself());
            firstProtocol.uponDeliverBlock(newFirst, firstRuntime.getMyself());
            secondProtocol.uponDeliverBlock(oldFirst, secondRuntime.getMyself());
            secondProtocol.uponDeliverBlock(oldSecond, secondRuntime.getMyself());

            assertEquals(longerBranchTransfer,
                    firstExecutions.poll(2, TimeUnit.SECONDS).transfer());
            assertEquals(Set.of(oldBranchTransfer.id()),
                    firstLedger.snapshot().executedTransferIds());
            assertEquals(Set.of(longerBranchTransfer.id()),
                    secondLedger.snapshot().executedTransferIds());
            assertNotEquals(firstLedger.stateDigest(), secondLedger.stateDigest());
            assertEquals(firstLedger.totalSupply(), firstLedger.sumOfBalances());
            assertEquals(secondLedger.totalSupply(), secondLedger.sumOfBalances());
        } finally {
            firstProtocol.stop();
            secondProtocol.stop();
            firstRuntime.shutdown();
            secondRuntime.shutdown();
        }
    }
}

package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import app.messages.Transfer;
import org.junit.jupiter.api.Test;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import replication.events.ExecuteTransaction;

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
}

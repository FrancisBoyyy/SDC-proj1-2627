package replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import app.messages.Transfer;
import org.junit.jupiter.api.Test;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import replication.events.ExecuteTransaction;

class BroadcastReplicationProtocolTest {

    @Test
    void deliversValidTransferOnlyOnceWhenBroadcastIsDuplicated() throws Exception {
        BlockingQueue<ExecuteTransaction> delivered = new LinkedBlockingQueue<>();
        Babel runtime = runtime(delivered);
        BroadcastReplicationProtocol protocol = new BroadcastReplicationProtocol(transfer -> true);
        Transfer transfer = transfer();

        try {
            protocol.init(runtime, null);

            protocol.uponDeliverTransfer(transfer, runtime.getMyself());
            protocol.uponDeliverTransfer(transfer, runtime.getMyself());

            ExecuteTransaction event = delivered.poll(2, TimeUnit.SECONDS);
            assertNotNull(event, "Valid transfer was not delivered");
            assertEquals(transfer, event.transfer());
            assertNull(delivered.poll(100, TimeUnit.MILLISECONDS));
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void ignoresInvalidTransfer() throws Exception {
        BlockingQueue<ExecuteTransaction> delivered = new LinkedBlockingQueue<>();
        Babel runtime = runtime(delivered);
        BroadcastReplicationProtocol protocol = new BroadcastReplicationProtocol(transfer -> false);

        try {
            protocol.init(runtime, null);
            protocol.uponDeliverTransfer(transfer(), runtime.getMyself());

            assertNull(delivered.poll(100, TimeUnit.MILLISECONDS));
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void rejectedDeliveryDoesNotPreventLaterValidationAndExecution() throws Exception {
        BlockingQueue<ExecuteTransaction> delivered = new LinkedBlockingQueue<>();
        Babel runtime = runtime(delivered);
        AtomicBoolean valid = new AtomicBoolean(false);
        BroadcastReplicationProtocol protocol = new BroadcastReplicationProtocol(transfer -> valid.get());
        Transfer transfer = transfer();

        try {
            protocol.init(runtime, null);

            protocol.uponDeliverTransfer(transfer, runtime.getMyself());
            valid.set(true);
            protocol.uponDeliverTransfer(transfer, runtime.getMyself());

            ExecuteTransaction event = delivered.poll(2, TimeUnit.SECONDS);
            assertNotNull(event, "Valid transfer was not delivered after rejection");
            assertEquals(transfer, event.transfer());
            assertNull(delivered.poll(100, TimeUnit.MILLISECONDS));
        } finally {
            runtime.shutdown();
        }
    }

    private static Babel runtime(BlockingQueue<ExecuteTransaction> delivered) {
        return Babel.builder()
                .onDeliver(ExecuteTransaction.class, delivered::add)
                .build()
                .init();
    }

    private static Transfer transfer() {
        return new Transfer("client1", "client2", 1, 0, "client1", new byte[] {1});
    }
}

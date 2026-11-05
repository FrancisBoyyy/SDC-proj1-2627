package broadcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import broadcast.messages.EchoBroadcastMessage;
import broadcast.messages.ReadyBroadcastMessage;
import broadcast.messages.SendBroadcastMessage;
import org.junit.jupiter.api.Test;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.events.BroadcastDeliverEvent;

class ByzantineReliableBroadcastProtocolTest {
    private static final int F = 1;
    private static final List<BabelNode> MEMBERS = List.of(
            new TestNode("node1"), new TestNode("node2"),
            new TestNode("node3"), new TestNode("node4"));

    @Test
    void deliversAfterTwoFPlusOneReadyVotesAndOnlyOnce() throws Exception {
        BlockingQueue<BroadcastDeliverEvent> delivered = new LinkedBlockingQueue<>();
        Babel runtime = runtime(MEMBERS.get(0), delivered);
        ByzantineReliableBroadcastProtocol protocol = protocol();
        SendBroadcastMessage message = message(new TestPayload("accepted"));

        try {
            protocol.init(runtime, null);

            protocol.uponEcho(new EchoBroadcastMessage(message), MEMBERS.get(0));
            protocol.uponEcho(new EchoBroadcastMessage(message), MEMBERS.get(1));
            protocol.uponEcho(new EchoBroadcastMessage(message), MEMBERS.get(2));
            assertNull(delivered.poll(100, TimeUnit.MILLISECONDS),
                    "ECHO quorum alone must not deliver the message");

            protocol.uponReady(new ReadyBroadcastMessage(message), MEMBERS.get(1));
            assertNull(delivered.poll(100, TimeUnit.MILLISECONDS),
                    "Delivery must wait for 2f+1 READY votes");
            protocol.uponReady(new ReadyBroadcastMessage(message), MEMBERS.get(2));

            BroadcastDeliverEvent event = delivered.poll(2, TimeUnit.SECONDS);
            assertNotNull(event, "Broadcast was not delivered after 2f+1 READY votes");
            assertEquals(message.payload(), event.message());
            assertEquals(message.origin(), event.sender());

            protocol.uponReady(new ReadyBroadcastMessage(message), MEMBERS.get(3));
            assertNull(delivered.poll(100, TimeUnit.MILLISECONDS),
                    "A delivered instance must not be delivered a second time");
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void ignoresEquivocatingMessagesFromOneAlreadyCountedMember() throws Exception {
        BlockingQueue<BroadcastDeliverEvent> delivered = new LinkedBlockingQueue<>();
        Babel runtime = runtime(MEMBERS.get(0), delivered);
        ByzantineReliableBroadcastProtocol protocol = protocol();
        UUID id = UUID.randomUUID();
        SendBroadcastMessage first = new SendBroadcastMessage(
                id, MEMBERS.get(0), new TestPayload("first"));
        SendBroadcastMessage conflicting = new SendBroadcastMessage(
                id, MEMBERS.get(0), new TestPayload("conflicting"));

        try {
            protocol.init(runtime, null);

            protocol.uponEcho(new EchoBroadcastMessage(first), MEMBERS.get(0));
            protocol.uponEcho(new EchoBroadcastMessage(first), MEMBERS.get(1));
            protocol.uponEcho(new EchoBroadcastMessage(first), MEMBERS.get(2));

            // node4 is the single Byzantine member and sends conflicting votes.
            protocol.uponEcho(new EchoBroadcastMessage(conflicting), MEMBERS.get(3));
            protocol.uponReady(new ReadyBroadcastMessage(conflicting), MEMBERS.get(3));
            protocol.uponReady(new ReadyBroadcastMessage(conflicting), MEMBERS.get(3));

            assertNull(delivered.poll(200, TimeUnit.MILLISECONDS),
                    "Conflicting values from the same voters must not form a second quorum");
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void ignoresReadyVotesFromNonMembers() throws Exception {
        BlockingQueue<BroadcastDeliverEvent> delivered = new LinkedBlockingQueue<>();
        Babel runtime = runtime(MEMBERS.get(0), delivered);
        ByzantineReliableBroadcastProtocol protocol = protocol();
        SendBroadcastMessage message = message(new TestPayload("membership"));
        TestNode outsider = new TestNode("outsider");

        try {
            protocol.init(runtime, null);

            protocol.uponReady(new ReadyBroadcastMessage(message), MEMBERS.get(1));
            protocol.uponReady(new ReadyBroadcastMessage(message), outsider);

            assertNull(delivered.poll(200, TimeUnit.MILLISECONDS),
                    "A non-member must not count toward the READY quorum");
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void relaysReadyMessagesThatArriveBeforeEcho() throws Exception {
        BlockingQueue<BroadcastDeliverEvent> delivered = new LinkedBlockingQueue<>();
        Babel runtime = runtime(MEMBERS.get(0), delivered);
        ByzantineReliableBroadcastProtocol protocol = protocol();
        SendBroadcastMessage message = message(new TestPayload("ready-first"));

        try {
            protocol.init(runtime, null);

            protocol.uponReady(new ReadyBroadcastMessage(message), MEMBERS.get(1));
            protocol.uponReady(new ReadyBroadcastMessage(message), MEMBERS.get(1));
            assertNull(delivered.poll(100, TimeUnit.MILLISECONDS),
                    "A duplicate READY from one member must count only once");

            protocol.uponReady(new ReadyBroadcastMessage(message), MEMBERS.get(2));

            BroadcastDeliverEvent event = delivered.poll(2, TimeUnit.SECONDS);
            assertNotNull(event, "READY relay did not deliver after the 2f+1 threshold");
            assertEquals(message.payload(), event.message());
            assertEquals(message.origin(), event.sender());
        } finally {
            runtime.shutdown();
        }
    }

    private static ByzantineReliableBroadcastProtocol protocol() {
        return new ByzantineReliableBroadcastProtocol(MEMBERS, F);
    }

    private static Babel runtime(
            BabelNode myself,
            BlockingQueue<BroadcastDeliverEvent> delivered) {
        return Babel.builder()
                .setMyself(myself)
                .onDeliver(BroadcastDeliverEvent.class, delivered::add)
                .build()
                .init();
    }

    private static SendBroadcastMessage message(BabelMessage payload) {
        return new SendBroadcastMessage(UUID.randomUUID(), MEMBERS.get(0), payload);
    }

    private record TestNode(String name) implements BabelNode {
    }

    private record TestPayload(String value) implements BabelMessage {
    }
}

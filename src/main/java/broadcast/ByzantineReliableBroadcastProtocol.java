package broadcast;

import broadcast.messages.SendBroadcastMessage;
import broadcast.messages.EchoBroadcastMessage;
import broadcast.messages.ReadyBroadcastMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastProviderBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipConsumerBabelProtocol;

import java.util.*;

/**
 * Byzantine reliable broadcast over authenticated reliable links.
 *
 * Payloads must have content-based equals/hashCode and must not be
 * mutated after submission.
 */
public class ByzantineReliableBroadcastProtocol
        implements BroadcastProviderBabelProtocol, MembershipConsumerBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(ByzantineReliableBroadcastProtocol.class);

    private final Set<BabelNode> members;

    private final int echoQuorum;
    private final int readyQuorum;
    private final int deliveryQuorum;

    private final Set<BabelNode> neighbors = new HashSet<>();
    private final Map<BroadcastInstanceId, BroadcastState> instances = new HashMap<>();

    /**
     * Relevant objects to communicate with the Babel 2 framework.
     * The runtime object is for intra machine protocol communication. The channel
     * object is for inter machine communication.
     * The Babel object can only have one implementation. while BabelChannel can
     * have many. Ideally, the protocol code should not know what is the specific
     * BabelChannel implementation being used, only what guarantees it provides.
     */
    private Babel runtime;
    private BabelChannel channel;

    public ByzantineReliableBroadcastProtocol(List<BabelNode> membership, int f) {
        members = Set.copyOf(membership);

        if (members.isEmpty()
                || members.size() != membership.size()
                || f < 0
                || members.size() < 3 * f + 1) {
            throw new IllegalArgumentException(
                    "Require distinct replicas, f >= 0, and n >= 3f + 1");
        }

        echoQuorum = ((members.size() +  f) / 2 + 1);
        readyQuorum = f + 1;
        deliveryQuorum = 2 * f + 1;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        if (!members.contains(runtime.getMyself())) {
            throw new IllegalStateException(
                    "This process is not a configured replica");
        }
        this.runtime = runtime;
        this.channel = channel;
    }

    @Override
    public void uponBroadcastRequest(BabelMessage payload) {
        LOGGER.info("Broadcast request for {}... sending to current membership", payload);
        var message = new SendBroadcastMessage(
                UUID.randomUUID(), runtime.getMyself(), payload);

        var state = stateFor(message);
        state.send = message;

        sendToNeighbors(message);
        sendEcho(state, message);
    }

    @UponMessageIn
    public void uponSend(SendBroadcastMessage message, BabelNode sender) {
        if (!accept(message, sender)
                || !sender.equals(message.origin())) {
            return;
        }

        var state = stateFor(message);
        if (!state.delivered) {
            sendEcho(state, message);
        }
    }

    @UponMessageIn
    public void uponEcho(EchoBroadcastMessage echo, BabelNode sender) {
        var message = echo.message();
        if (!accept(message, sender))
            return;

        var state = stateFor(message);
        if (state.delivered)
            return;

        if (state.echoes.putIfAbsent(sender, message) == null)
            advance(state, message);
    }

    @UponMessageIn
    public void uponReady(ReadyBroadcastMessage ready, BabelNode sender) {
        var message = ready.message();
        if (!accept(message, sender))
            return;

        var state = stateFor(message);
        if (state.delivered)
            return;

        if (state.readies.putIfAbsent(sender, message) == null)
            advance(state, message);
    }

    private boolean accept(SendBroadcastMessage message, BabelNode sender) {
        return members.contains(sender)
                && message != null
                && message.id() != null
                && message.origin() != null
                && members.contains(message.origin())
                && message.payload() != null;
    }

    private BroadcastState stateFor(SendBroadcastMessage message) {
        var id = new BroadcastInstanceId(message.origin(), message.id());
        return instances.computeIfAbsent(id, ignored -> new BroadcastState());
    }

    private void sendEcho(BroadcastState state, SendBroadcastMessage message) {
        if (state.echo != null)
            return;

        state.echo = message;
        state.echoes.put(runtime.getMyself(), message);

        LOGGER.info("Sending ECHO for {}", message);
        sendToNeighbors(new EchoBroadcastMessage(message));
        advance(state, message);
    }

    /**
     * Whenever a message gets a ECHO or READY vote this method is
     * called to advance the Broadcast State.
     * @param state Broadcast state
     * @param candidate message who got a vote
     */
    private void advance(BroadcastState state, SendBroadcastMessage candidate) {
        long echoes = BroadcastState.count(state.echoes, candidate);
        long readies = BroadcastState.count(state.readies, candidate);

        if (state.ready == null && (echoes >= echoQuorum || readies >= readyQuorum)) {
            state.ready = candidate;
            state.readies.put(runtime.getMyself(), candidate);
            LOGGER.info("Sending READY for {}", candidate);
            sendToNeighbors(new ReadyBroadcastMessage(candidate));
        }

        // Our own READY may have increased this count.
        readies = BroadcastState.count(state.readies, candidate);

        if (!state.delivered && readies >= deliveryQuorum) {
            state.delivered = true;
            LOGGER.info("Delivering {}", candidate);
            deliverMessage(runtime, candidate.payload(), candidate.origin());
        }
    }

    @Override
    public void neighbourUp(BabelNode node) {
        if (!members.contains(node)
                || node.equals(runtime.getMyself())
                || !neighbors.add(node)) {
            return;
        }

        // A peer may connect after we emitted one or more phases, retransmit
        for (var state : instances.values()) {
            var send = state.send;
            var echo = state.echo;
            var ready = state.ready;

            if (send != null) channel.send(node, send);
            if (echo != null) channel.send(node, new EchoBroadcastMessage(echo));
            if (ready != null) channel.send(node, new ReadyBroadcastMessage(ready));

        }

        LOGGER.info("NEIGHBOR UP: {}", node);
    }

    @Override
    public void neighbourDown(BabelNode babelNode) {
        LOGGER.info("NEIGHBOR DOWN: {}", babelNode);
        neighbors.remove(babelNode);
    }

    private void sendToNeighbors(BabelMessage message) {
        for (var neighbor : neighbors) {
            this.channel.send(neighbor, message);
        }
    }
}
package broadcast;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import broadcast.messages.BroadcastMessage;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastProviderBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipConsumerBabelProtocol;

/**
 * Eager Reliable Broadcast (as discussed in the lectures), for the crash fault
 * model. It is provided as an example of a broadcast provider: the protocol
 * that requests the broadcast uses {@code broadcast(runtime, message)}
 * (brbBroadcast) and handles deliveries with {@code @UponBroadcastDeliver}
 * (brbDeliver).
 *
 * <p>
 * This protocol is NOT tolerant to Byzantine faults. Among other problems: a
 * faulty process can forge the origin of a message, send different messages
 * with the same id to different processes (equivocation), or relay a message
 * to only some processes. You have to replace it by a Byzantine Reliable
 * Broadcast protocol with the same interface.
 */
public class CrashFaultReliableBroadcastProtocol
        implements BroadcastProviderBabelProtocol, MembershipConsumerBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(CrashFaultReliableBroadcastProtocol.class);
    public static final int DEFAULT_EXPECTED_MESSAGES = 1024;

    private final Set<BabelNode> members;
    private final Set<UUID> delivered;
    private final Set<BabelNode> neighbors;

    /**
     * Relevant objects to communicate with the Babel 2 framework.
     * The runtime object is for intra machine protocol communication. The channel
     * object is for inter machine communication.
     */
    private Babel runtime;
    private BabelChannel channel;

    /**
     * @param membership the (static) set of replicas. Messages received from any
     *                   other process are ignored.
     */
    public CrashFaultReliableBroadcastProtocol(List<BabelNode> membership) {
        this.members = Set.copyOf(membership);
        this.delivered = Collections.newSetFromMap(HashMap.newHashMap(DEFAULT_EXPECTED_MESSAGES));
        this.neighbors = Collections.newSetFromMap(HashMap.newHashMap(membership.size()));
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
    }

    /**
     * Request from the protocol above: deliver the message locally and send it
     * to everyone.
     */
    @Override
    public void uponBroadcastRequest(BabelMessage message) {
        var broadcastMessage = new BroadcastMessage(UUID.randomUUID(), runtime.getMyself(), message);
        LOGGER.debug("Broadcast request for {}... sending to current membership", message);
        delivered.add(broadcastMessage.id());
        this.deliverMessage(runtime, message, runtime.getMyself());
        sendToEveryone(broadcastMessage);
    }

    /**
     * Check if this broadcast message has been seen and retransmit it if it has
     * not (so that, if the origin crashed in the middle of the broadcast, every
     * correct process still gets the message).
     */
    @UponMessageIn
    public void uponBroadcastMessage(BroadcastMessage message, BabelNode sender) {
        if (!members.contains(sender)) {
            // Clients can also send messages to replicas: never trust them!
            LOGGER.warn("Ignoring broadcast message sent by {}, which is not a replica", sender);
            return;
        }
        if (delivered.add(message.id())) {
            LOGGER.debug("New message {} from {}, retransmitting it...", message.id(), sender);
            sendToEveryone(message);
            this.deliverMessage(runtime, message.payload(), message.origin());
        }
    }

    @Override
    public void neighbourUp(BabelNode node) {
        LOGGER.debug("NEIGHBOR UP: {}", node);
        this.neighbors.add(node);
    }

    @Override
    public void neighbourDown(BabelNode node) {
        LOGGER.debug("NEIGHBOR DOWN: {}", node);
        this.neighbors.remove(node);
    }

    private void sendToEveryone(BabelMessage message) {
        for (var neighbor : this.neighbors) {
            this.channel.send(neighbor, message);
        }
    }
}

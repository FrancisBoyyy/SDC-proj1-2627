package broadcast;

import broadcast.messages.BroadcastMessage;
import broadcast.messages.EchoBroadcastMessage;
import broadcast.utils.EchoEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.SignedBabelMessage;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastProviderBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipConsumerBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.signed.SignedMessageBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.signed.annotations.UponSignedMessageIn;

import java.util.*;

public class ByzantineConsistentBroadcastProtocol
        implements BroadcastProviderBabelProtocol, MembershipConsumerBabelProtocol, SignedMessageBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(ByzantineConsistentBroadcastProtocol.class);
    public static final int DEFAULT_EXPECTED_MESSAGES = 20;
    public static final int DEFAULT_EXPECTED_NEIGHBORS = 4;
    public static final int DEFAULT_F_NODES = 1;

    private final Set<UUID> delivered;
    private final Map<UUID, BabelNode> received;
    private final Map<UUID, Set<EchoEntry>> echos;
    private final Set<BabelNode> neighbors;

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

    public ByzantineConsistentBroadcastProtocol() {
        this(DEFAULT_EXPECTED_MESSAGES, DEFAULT_EXPECTED_NEIGHBORS, DEFAULT_F_NODES);
    }

    public ByzantineConsistentBroadcastProtocol(int expectedMessages, int expectedNeighbors, int fNodes) {
        delivered = Collections.newSetFromMap(HashMap.newHashMap(expectedMessages));
        received = new HashMap<UUID, BabelNode>();
        echos = new HashMap<UUID, Set<EchoEntry>>();
        neighbors = Collections.newSetFromMap(HashMap.newHashMap(expectedNeighbors));
    }

    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
    }

    /**
     * Redirect it to uponBroadcastMessage
     * 
     * @param message
     * @param sender
     */
    @UponSignedMessageIn
    public void uponEchoBroadcastMessage(SignedBabelMessage<EchoBroadcastMessage> message, BabelNode sender) {
        LOGGER.info("New echo message {} from {}, retransmitting it...", message.innerMessage(), sender);
        UUID mid = message.innerMessage().message().innerMessage().id();
        BabelNode p = sender;
        BabelMessage m = message.innerMessage().message();
        if (echos.containsKey(mid)) {
            echos.get(mid).add(new EchoEntry(p, m));
        } else {
            Set<EchoEntry> newSet = new HashSet<>();
            newSet.add(new EchoEntry(p, m));
            echos.put(mid, newSet);
        }
        checkIfCanDeliver(mid);
    }

    /**
     * Check if this broadcast message has been seen and retransmit it if it has
     * not.
     * 
     * @param message
     * @param sender
     */
    @UponSignedMessageIn
    public void uponBroadcastMessage(SignedBabelMessage<BroadcastMessage> message, BabelNode sender) {
        UUID mid = message.innerMessage().id();
        if (received.putIfAbsent(mid, sender) == null) {
            LOGGER.info("New message {} from {}, retransmitting it...", message.innerMessage(), sender);
            // We need to encapsulate the BroadcastMessage in a new SignedBabelMessage so
            // that the channel generates a new signature
            var broadcastMessage = new SignedBabelMessage<>(new EchoBroadcastMessage(message));
            sendToEveryone(broadcastMessage);
        }
        checkIfCanDeliver(mid);
    }

    private void checkIfCanDeliver(UUID mid) {
        Set<EchoEntry> entries = echos.get(mid);
        if (entries == null || received.get(mid)!=null) {
            return;
        }
        HashMap<BabelMessage, Integer> counts = new HashMap<>();
        for (EchoEntry entry : entries) {
            BabelMessage msg = entry.m();
            counts.put(msg, counts.getOrDefault(msg, 0) + 1);
        }
        int quorum = (2 * DEFAULT_F_NODES) + 1;
        for (Map.Entry<BabelMessage, Integer> countEntry : counts.entrySet()) {
            if (countEntry.getValue() >= quorum) {
                BabelMessage matchMessage = countEntry.getKey();
                delivered.add(mid);
                BabelNode originalSender = received.get(mid);
                this.deliverMessage(runtime, matchMessage, originalSender);
                break;
            }
        }
    }

    @Override
    public void uponBroadcastRequest(BabelMessage message) {
        LOGGER.info("Broadcast request for {}... sending to current membership", message);
        var broadcastMessage = new SignedBabelMessage<>(
                new BroadcastMessage(UUID.randomUUID(), runtime.getMyself(), message));
        sendToEveryone(broadcastMessage);
    }

    @Override
    public void neighbourUp(BabelNode node) {
        LOGGER.info("NEIGHBOR UP: {}", node);
        this.neighbors.add(node);
    }

    @Override
    public void neighbourDown(BabelNode babelNode) {
        LOGGER.info("NEIGBOR DOWN: {}", babelNode);
        this.neighbors.remove(babelNode);
    }

    private void sendToEveryone(BabelMessage message) {
        for (var neighbor : this.neighbors) {
            this.channel.send(neighbor, message);
        }
    }
}

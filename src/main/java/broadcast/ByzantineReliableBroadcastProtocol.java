package broadcast;

import broadcast.messages.BroadcastMessage;
import broadcast.messages.EchoBroadcastMessage;
import broadcast.messages.ReadyBroadcastMessage;
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

/**
 * Assume N >= 3f + 1, where N = neighbors + 1 (this node included) and f is the maximum number of Byzantine nodes.
 *
 * Phases:
 *  - SEND(m):  origin -> all
 *  - ECHO(m):  on first valid SEND(m), everyone sends ECHO(m) to all
 *  - READY(m): on > (N+f)/2 matching ECHOs OR on f+1 matching READYs, send READY(m) to all (only once)
 *  - deliver(m): on 2f+1 matching READYs
 */
public class ByzantineReliableBroadcastProtocol
        implements BroadcastProviderBabelProtocol, MembershipConsumerBabelProtocol, SignedMessageBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(ByzantineReliableBroadcastProtocol.class);
    public static final int DEFAULT_EXPECTED_MESSAGES = 20;
    public static final int DEFAULT_EXPECTED_NEIGHBORS = 4;
    public static final int DEFAULT_F_NODES = 1;

    private final int f;

    // Message ids for which this node already sent echo (at most once).
    private final Set<UUID> sentEcho;

    // Message ids for which this node already sent ready (at most once).
    private final Set<UUID> sentReady;

    // Message ids for which this node already delivered (at most once).
    private final Set<UUID> delivered;

    private final Map<UUID, Map<BabelNode, BroadcastMessage>> echos;
    private final Map<UUID, Map<BabelNode, BroadcastMessage>> readys;

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

    public ByzantineReliableBroadcastProtocol() {
        this(DEFAULT_EXPECTED_MESSAGES, DEFAULT_EXPECTED_NEIGHBORS, DEFAULT_F_NODES);
    }

    public ByzantineReliableBroadcastProtocol(int expectedMessages, int expectedNeighbors, int fNodes) {
        this.f = fNodes;
        sentEcho = new HashSet<>(expectedMessages);
        sentReady = new HashSet<>(expectedMessages);
        delivered = Collections.newSetFromMap(HashMap.newHashMap(expectedMessages));
        echos = new HashMap<>(expectedMessages);
        readys = new HashMap<>(expectedMessages);
        neighbors = Collections.newSetFromMap(HashMap.newHashMap(expectedNeighbors));
    }

    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
    }

    private int n() {
        return neighbors.size() + 1;
    }

    private int echoQuorum() {
        return (n() + f) / 2 + 1;
    }

    private int readyQuorum() {
        return f + 1;
    }

    private int deliverQuorum() {
        return 2 * f + 1;
    }

    @UponSignedMessageIn
    public void uponBroadcastMessage(SignedBabelMessage<BroadcastMessage> message, BabelNode sender) {
        BroadcastMessage bm = message.innerMessage();
        if (!sender.equals(bm.origin())) {
            LOGGER.warn("Ignoring SEND {} from {}: sender is not the origin {}", bm, sender, bm.origin());
            return;
        }
        if (delivered.contains(bm.id())) {
            return;
        }
        LOGGER.info("New SEND {} from {}", bm, sender);
        sendEcho(message);
    }

    @UponSignedMessageIn
    public void uponEchoBroadcastMessage(SignedBabelMessage<EchoBroadcastMessage> message, BabelNode sender) {
        SignedBabelMessage<BroadcastMessage> original = message.innerMessage().message();
        BroadcastMessage bm = original.innerMessage();
        UUID mid = bm.id();
        if (delivered.contains(mid)) {
            return;
        }
        LOGGER.info("ECHO for {} from {}", bm, sender);
        recordVote(echos, mid, sender, bm);
        checkEchoQuorum(original);
    }

    @UponSignedMessageIn
    public void uponReadyBroadcastMessage(SignedBabelMessage<ReadyBroadcastMessage> message, BabelNode sender) {
        SignedBabelMessage<BroadcastMessage> original = message.innerMessage().message();
        BroadcastMessage bm = original.innerMessage();
        UUID mid = bm.id();
        if (delivered.contains(mid)) {
            return;
        }
        LOGGER.info("READY for {} from {}", bm, sender);
        recordVote(readys, mid, sender, bm);
        checkReadyQuorums(original);
    }

    @Override
    public void uponBroadcastRequest(BabelMessage message) {
        LOGGER.info("Broadcast request for {}... sending to current membership", message);
        var signed = new SignedBabelMessage<>(
                new BroadcastMessage(UUID.randomUUID(), runtime.getMyself(), message));
        sendToEveryone(signed);
        sendEcho(signed);
    }

    private void sendEcho(SignedBabelMessage<BroadcastMessage> original) {
        BroadcastMessage bm = original.innerMessage();
        UUID mid = bm.id();
        if (!sentEcho.add(mid)) {
            return;
        }
        LOGGER.info("Sending ECHO for {}", bm);
        recordVote(echos, mid, runtime.getMyself(), bm);
        sendToEveryone(new SignedBabelMessage<>(new EchoBroadcastMessage(original)));
        checkEchoQuorum(original);
    }

    private void sendReady(SignedBabelMessage<BroadcastMessage> original) {
        BroadcastMessage bm = original.innerMessage();
        UUID mid = bm.id();
        if (!sentReady.add(mid)) {
            return;
        }
        LOGGER.info("Sending READY for {}", bm);
        recordVote(readys, mid, runtime.getMyself(), bm);
        sendToEveryone(new SignedBabelMessage<>(new ReadyBroadcastMessage(original)));
        checkReadyQuorums(original);
    }

    private void checkEchoQuorum(SignedBabelMessage<BroadcastMessage> original) {
        BroadcastMessage bm = original.innerMessage();
        if (count(echos, bm.id(), bm) >= echoQuorum()) {
            sendReady(original);
        }
    }

    // f+1 readys => send READY; 2f+1 readys => deliver.
    private void checkReadyQuorums(SignedBabelMessage<BroadcastMessage> original) {
        BroadcastMessage bm = original.innerMessage();
        UUID mid = bm.id();
        long c = count(readys, mid, bm);

        if (c >= readyQuorum()) {
            sendReady(original); // no-op if already sent
        }
        // Recount: sendReady above may have added my own vote
        c = count(readys, mid, bm);
        if (c >= deliverQuorum() && delivered.add(mid)) {
            LOGGER.info("Delivering {}", bm);
            this.deliverMessage(runtime, bm.payload(), bm.origin());
        }
    }

    private void recordVote(Map<UUID, Map<BabelNode, BroadcastMessage>> votes,
                            UUID mid, BabelNode from, BroadcastMessage m) {
        votes.computeIfAbsent(mid, k -> new HashMap<>()).putIfAbsent(from, m);
    }

    private long count(Map<UUID, Map<BabelNode, BroadcastMessage>> votes, UUID mid, BroadcastMessage m) {
        return votes.getOrDefault(mid, Map.of()).values().stream().filter(m::equals).count();
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
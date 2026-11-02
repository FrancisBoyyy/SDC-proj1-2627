package agreement;

import agreement.events.Decided;
import agreement.events.Propose;
import agreement.messages.CommitMessage;
import agreement.messages.PrePrepareMessage;
import agreement.messages.PrepareMessage;
import agreement.messages.ProposeMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;

import java.util.*;

public class PracticalByzantineFaultTolerantProtocol implements BabelProtocol {

    private static final Logger LOGGER = LogManager.getLogger(PracticalByzantineFaultTolerantProtocol.class);

    private final List<BabelNode> neighbors;

    private final int prepareQuorum;
    private final int commitQuorum;

    private final Map<PBFTInstanceId, PBFTState> instances;

    private int currentView; // TODO : viewChange
    private int currentSeqn; // monotonic increasing sequence number, only for primary usage!

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

    public PracticalByzantineFaultTolerantProtocol(List<BabelNode> membership, int f) {
        if (f < 0 || membership.isEmpty() || membership.size() < 3 * f + 1)
            throw new IllegalArgumentException(
                    "Require distinct replicas, f >= 0, and N >= 3f + 1");

        this.neighbors = new ArrayList<>(membership);

        this.prepareQuorum = 2*f + 1; // the implementation includes the primary in the quorum threshold
        this.commitQuorum = 2*f + 1;

        this.instances = new HashMap<>();

        this.currentView = 0; // by convention!
        this.currentSeqn = 0; // by convention!
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        if (!neighbors.contains(runtime.getMyself())) {
            throw new IllegalStateException(
                    "This process is not a configured replica");
        }
        this.runtime = runtime;
        this.channel = channel;
    }

    // aux methods

    private BabelNode expectedPrimary() {
        return neighbors.get(currentView % neighbors.size());
    }

    private PBFTState stateFor(long view, long seqn) {
        PBFTInstanceId id = new PBFTInstanceId(view, seqn);
        return instances.computeIfAbsent(id, ignored -> new PBFTState());
    }

    private void sendToNeighbors(BabelMessage message, @Nullable BabelNode ignore) {
        for (BabelNode node : neighbors)
            if (!node.equals(ignore))
                channel.send(node, message);
    }

    private void sendToOthers(BabelMessage m) {
        sendToNeighbors(m, runtime.getMyself());
    }

    private void tryPrepared(PBFTState state, long view, long seqn) {
        if (state.getPrepareSize() < prepareQuorum)
            return;

        state.setPrepared();

        CommitMessage commit = new CommitMessage(view, seqn, state.getDigest());
        LOGGER.info("Prepares quorum reached -> Commit {} sent", commit);
        state.registerCommit(runtime.getMyself(), commit);   // own commit
        sendToNeighbors(commit, null);

        tryCommitted(state, seqn);   // commits may have arrived before we were prepared
    }

    private void tryCommitted(PBFTState state, long seqn) {
        if (state.getCommitSize() < commitQuorum)
            return;
        state.setCommited();

        Decided decided = new Decided(seqn, state.getBatch());
        LOGGER.info("Commits quorum reached -> Decide {} sent to SMR", decided);
        runtime.deliverEvent(decided);
    }

    private void processPropose(List<BabelMessage> ms) {
        //handle the propose, send the Pre-Prepare to all replicas containing the propose operation, a seqn,
        //  currentView and the hash of the propose.value

        String hash = PBFTState.operationDigest(ms);
        currentSeqn++;
        PrePrepareMessage preprepareMsg = new PrePrepareMessage(ms, currentView, currentSeqn, hash);

        PBFTState state = stateFor(preprepareMsg.view(), preprepareMsg.seqn());
        state.setPrePrepared(ms);

        String batchStr = Arrays.toString(ms.toArray());
        LOGGER.info("Propose request for {}... sending to current membership with (v, n, d)=({}, {}, {})",
                batchStr, currentView, currentSeqn, hash);
        sendToNeighbors(preprepareMsg, runtime.getMyself());
    }

    @UponBabelEvent
    public void uponPropose(Propose propose) {
        LOGGER.info("Receive propose myself (primary)");
        processPropose(propose.values());
    }

    @UponMessageIn
    public void uponPrePrepareMsg(PrePrepareMessage msg, BabelNode sender) {
        LOGGER.info("Pre-prepare received from {}", sender);

        // verify the conditions for accepting that PrePrepare:
        // 1. the sender must be the primary with index in neighbors p = currentView % neighbors.size()
        // msg = operation, view, seqn and hash. Confirm that
        //   2.1 hash=Hash(operation),
        //   2.2 view = currentView
        //   2.3 instances does not have the entry (view, seqn) with a PBFT state such that false=canAcceptPrePrepare(hash)
        //   2.4 seqn don't exhaust the number space

        // 2.2
        if (currentView != msg.view()) {
            LOGGER.warn("currentView={} != msg.view={}", currentView, msg.view());
            return;
        }

        // 1.
        if (!sender.equals(expectedPrimary())) {
            LOGGER.warn("sender={} is not the primary={}", sender, expectedPrimary());
            return;
        }

        // 2.1
        String expectedHash = PBFTState.operationDigest(msg.ms());
        if (!msg.hash().equals(expectedHash)) {
            LOGGER.warn("msg.hash()={} != expectedHash={}", msg.hash(), expectedHash);
            return;
        }

        // 2.3
        PBFTState state = stateFor(msg.view(), msg.seqn());
        if (!state.canAcceptPrePrepare(expectedHash)) {
            LOGGER.warn("This replica previously accept a PrePrepare with a different hash.");
            return;
        }

        // TODO: 2.4...

        // all conditions verified -> accept the pre-prepare!
        state.setPrePrepared(msg.ms());

        PrepareMessage prepare = new PrepareMessage(msg.view(), msg.seqn(), expectedHash);
        LOGGER.info("Accepted PrePrepare -> Prepare {} sent", prepare);
        state.registerPrepare(runtime.getMyself(), prepare);   // own prepare
        sendToNeighbors(prepare, null);

        tryPrepared(state, msg.view(), msg.seqn());            // other prepares may be buffered
    }

    @UponMessageIn
    public void uponPrepareMsg(PrepareMessage msg, BabelNode sender) {
        if (!neighbors.contains(sender) || sender.equals(expectedPrimary()))
            return;   // the primary's vote is its PrePrepare

        PBFTState state = stateFor(msg.view(), msg.seqn());
        state.registerPrepare(sender, msg);
        tryPrepared(state, msg.view(), msg.seqn());
    }

    @UponMessageIn
    public void uponCommitMsg(CommitMessage msg, BabelNode sender) {
        if (!neighbors.contains(sender))
            return;

        PBFTState state = stateFor(msg.view(), msg.seqn());
        state.registerCommit(sender, msg);
        tryCommitted(state, msg.seqn());
    }
}

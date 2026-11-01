package agreement;

import agreement.events.Propose;
import agreement.messages.CommitMessage;
import agreement.messages.PrePrepareMessage;
import agreement.messages.PrepareMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
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

    private int currentView;

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

    @UponBabelEvent
    public void uponPropose(Propose propose) {
        LOGGER.info("Propose request for {}... sending to current membership", propose);
        // TODO: handle the propose, send the Pre-Prepare to all replicas containing the propose operation, a seqn,
        //  currentView and the hash of the propose.value
    }

    @UponMessageIn
    public void uponPrePrepareMsg(PrePrepareMessage msg, BabelNode sender) {
        LOGGER.info("Pre-prepare received from {}", sender);

        // TODO: verify the conditions for accepting that PrePrepare:
        // 1. the sender must be the primary with index in neighbors p = currentView % neighbors.size()
        // msg = operation, view, seqn and hash. Confirm that
        //   2.1 hash=Hash(operation),
        //   2.2 view = currentView
        //   2.3 instances does not have the entry (view, seqn) with a PBFT state such that false=canAcceptPrePrepare(hash)
        //   2.4 seqn don't exhaust the number space

        // TODO: setPrePrepared(msg.value)
        // TODO: make the Prepare msg and send to all replicas (including itself and primary)
    }

    @UponMessageIn
    public void uponPrepareMsg(PrepareMessage msg, BabelNode sender) {
        LOGGER.info("Prepare received from {}", sender);

        // TODO: registerPrepare(sender, msg)
        // TODO: collect a majority quorum of prepares to transition to the PREPARED state that is setPrepared()
        // TODO: broadcast COMMIT
    }

    @UponMessageIn
    public void uponCommitMsg(CommitMessage msg, BabelNode sender) {
        LOGGER.info("Commit received from {}", sender);

        // TODO: registerCommit(sender, msg)
        // TODO: collect a majority quorum of commits to transition to the COMMIT state that is setCommit()

        // TODO: send to the SMR the indication Decided with that seqn
    }
}

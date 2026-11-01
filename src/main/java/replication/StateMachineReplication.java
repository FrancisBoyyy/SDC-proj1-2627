package replication;

import agreement.events.Decided;
import agreement.events.Propose;
import agreement.messages.ProposeMsg;
import app.messages.Transfer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Placeholder for the replication protocol, so that the application can be
 * executed (and tested with the client) before you implement anything.
 *
 * <p>
 * This logic is evidently incorrect: transfers are executed locally, as soon
 * as they are submitted, without being propagated to (or ordered with) the
 * other replicas. Replace this protocol by:
 * <ul>
 * <li>a simple replication protocol that uses Byzantine Reliable Broadcast
 * (variant a);</li>
 * <li>a State Machine Replication protocol that uses PBFT (variant b);</li>
 * <li>a blockchain protocol that uses Byzantine Reliable Broadcast to
 * disseminate blocks (variant c).</li>
 * </ul>
 */
public class StateMachineReplication implements BabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(StateMachineReplication.class);

    private final Set<String> submitted; // already submitted operations to Agreement protocol
    private long nextSeqn;
    private final Set<Transfer> pending; // pending operations to be executed

    private BabelNode primary;

    private Babel runtime;
    private BabelChannel channel;

    private final Predicate<Transfer> validTransfer;

    public StateMachineReplication(List<BabelNode> membership, Predicate<Transfer> validTransfer) {
        if (membership.isEmpty())
            throw new IllegalArgumentException("Require a non-empty membership");

        this.submitted = new HashSet<>();
        this.nextSeqn = 1L; // the first seqn conventioned to be executed
        this.pending = new HashSet<>();

        this.primary = membership.getFirst(); // by convention!

        this.validTransfer = validTransfer;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
    }

    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        Transfer transfer = submit.transfer();

        if (runtime.getMyself().equals(primary)) {
            if (submitted.add(transfer.id())) {
                LOGGER.info("Submitting transfer {}", transfer.id());
                runtime.deliverEvent(new Propose(transfer));
            }

        } else {
            channel.send(primary, new ProposeMsg(transfer));
        }
    }

    @UponBabelEvent
    public void uponDeliverTransfer(Decided decided) {
        Transfer transfer = (Transfer) decided.value();

        if (!validTransfer.test(transfer)) {
            LOGGER.warn("Ignoring invalid broadcast transfer");
            return;
        }

        if (decided.sqn() == nextSeqn) {
            LOGGER.info("Delivering transfer {}", transfer.id());
            runtime.deliverEvent(new ExecuteTransaction(transfer));
            nextSeqn++;

        } else {
            LOGGER.warn("Hole detected! Adding transfer {} to pending", transfer.id());
            pending.add(transfer);
        }

    }
}

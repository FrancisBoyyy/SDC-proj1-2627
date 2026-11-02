package replication;

import agreement.events.Decided;
import agreement.events.Propose;
import agreement.messages.ProposeMsg;
import app.messages.Transfer;
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
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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

    private final SMRParameters params;
    private List<BabelMessage> currentBatch;
    private @Nullable ScheduledFuture<?> batchTimer;

    private final Set<String> submitted; // already submitted operations to Agreement protocol
    private long nextSeqn;
    private final TreeMap<Long, List<Transfer>> pending;
    private BabelNode primary;

    private Babel runtime;
    private BabelChannel channel;

    private final Predicate<Transfer> validTransfer;
    public StateMachineReplication(List<BabelNode> membership, Predicate<Transfer> validTransfer, SMRParameters params) {
        if (membership.isEmpty())
            throw new IllegalArgumentException("Require a non-empty membership");

        this.params = params;
        this.currentBatch = new LinkedList<>();

        this.submitted = new HashSet<>();
        this.nextSeqn = 1L; // the first seqn conventioned to be executed
        this.pending = new TreeMap<>();

        this.primary = membership.getFirst(); // by convention!

        this.validTransfer = validTransfer;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;

        this.batchTimer = null;
    }

    private record DecidedTransferBatch(List<Transfer> ms, long seqn) {}

    @UponBabelEvent
    public void uponBatchTimer(BatchTimer timer) {
        batchTimer = null;
        submitCurrentBatch();
    }

    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        Transfer transfer = submit.transfer();

        if (!runtime.getMyself().equals(primary)) {
            LOGGER.info("Sending the transfer {} to primary {} because i am not the primary", transfer.id(), primary);
            channel.send(primary, new ProposeMsg(transfer));
            return;
        }

        // this replica is the primary
        submitTransaction(transfer);
    }

    private void submitTransaction(Transfer transfer) {
        if (submitted.add(transfer.id())) {
            currentBatch.add(transfer);
            if (currentBatch.size() == 1)
                batchTimer = runtime.setUpTimer(new BatchTimer(), params.batchTimeout(), TimeUnit.MILLISECONDS);
        }
        if (currentBatch.size() >= params.batchSize())
            submitCurrentBatch();
    }

    @UponMessageIn
    public void uponProposeMsg(ProposeMsg propose, BabelNode sender) {
        LOGGER.info("Receive propose from replica {}", sender);
        submitTransaction(propose.value());
    }

    private void submitCurrentBatch() {
        if (batchTimer != null) {
            batchTimer.cancel(false);
            batchTimer = null;
        }

        if (currentBatch.isEmpty())
            return;

        runtime.deliverEvent(new Propose(new ArrayList<>(currentBatch)));
        currentBatch.clear();
    }

    private void deliverPending() {
        List<Transfer> next;
        while ((next = pending.remove(nextSeqn)) != null) {
            next.forEach(t -> runtime.deliverEvent(new ExecuteTransaction(t)));
            nextSeqn++;
        }
    }

    @UponBabelEvent
    public void uponDeliverTransfer(Decided decided) {
        if (decided.sqn() < nextSeqn)
            return;

        List<Transfer> batch = new ArrayList<>();
        for (BabelMessage m : decided.batch()) {
            Transfer t = (Transfer) m;
            if (validTransfer.test(t))
                batch.add(t);

            else
                LOGGER.warn("Ignoring invalid transfer {} in batch {}", t.id(), decided.sqn());
        }

        pending.putIfAbsent(decided.sqn(), batch);
        deliverPending();
    }
}

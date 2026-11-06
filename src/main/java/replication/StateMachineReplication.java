package replication;

import agreement.events.Decided;
import agreement.events.Propose;
import agreement.events.SuspectPrimary;
import agreement.events.ViewChanged;
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
import replication.timers.BatchTimer;
import replication.timers.SuspectTimer;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * State Machine Replication protocol that uses PBFT (variant b).
 */
public class StateMachineReplication implements BabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(StateMachineReplication.class);

    private final SMRParameters params;
    private final List<BabelNode> membership;

    private List<BabelMessage> currentBatch;
    private @Nullable ScheduledFuture<?> batchTimer;

    private final Map<String, Transfer> unexecuted; // transfers that were not executed yet (all replicas)
    private @Nullable ScheduledFuture<?> suspectTimer;
    private long currentView;
    private long nextSeqn;
    private final TreeMap<Long, List<Transfer>> pending;
    private BabelNode primary;

    private Babel runtime;
    private BabelChannel channel;

    public StateMachineReplication(List<BabelNode> membership, SMRParameters params) {
        if (membership.isEmpty())
            throw new IllegalArgumentException("Require a non-empty membership");

        this.params = params;

        this.currentBatch = new LinkedList<>();
        this.currentView = 0L; // by convention!

        this.unexecuted  = new HashMap<>();
        this.nextSeqn = 1L; // the first seqn conventioned to be executed
        this.pending = new TreeMap<>();

        this.primary = membership.getFirst(); // by convention!
        this.membership = membership;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;

        this.batchTimer = null;
        this.suspectTimer = null;
    }

    // auxiliary methods

    /**
     * Returns the expected primary for the given view.
     */
    private BabelNode expectedPrimary(long view) {
        return membership.get(Math.floorMod(view, membership.size()));
    }

    /**
     * Evaluates if I am the current primary
     * @return true if I am the current primary, false otherwise.
     */
    private boolean myselfPrimary() {
        return runtime.getMyself().equals(primary);
    }

    // TIMERS

    /**
     * Start the suspect timer for the primary being faulty.
     */
    private void startSuspectTimer() {
        if (suspectTimer != null || myselfPrimary())
            return;

        suspectTimer = runtime.setUpTimer(new SuspectTimer(currentView), params.suspectTimeout(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the suspect timer for the primary being faulty.
     */
    private void stopSuspectTimer() {
        if (suspectTimer != null) {
            suspectTimer.cancel(false);
            suspectTimer = null;
        }
    }

    /**
     * Handler for the suspect timer.
     * When it fires, notifies the underlying protocol that the primary is being suspected as faulty
     * for the current view number.
     */
    @UponBabelEvent
    public void uponSuspectTimer(SuspectTimer timer) {
        suspectTimer = null;
        if (timer.view() < currentView)
            return; // stale timer

        LOGGER.warn("Request timeout in view {}, suspecting that primary {} is faulty", currentView, primary);
        runtime.deliverEvent(new SuspectPrimary(currentView));
    }

    /**
     * Fires when there is no activity from the clients, so submit the current catch as it is (incomplete).
     */
    @UponBabelEvent
    public void uponBatchTimer(BatchTimer timer) {
        batchTimer = null;
        submitCurrentBatch();
    }

    // NOTIFICATIONS

    /**
     * App level transfer submissions. They need to be ordered before indicating them
     * back to the application.
     * Submit the transaction if the replica is the primary, otherwise
     * redirects it to the current primary while logging the transfer as unexecuted.
     */
    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        Transfer transfer = submit.transfer();

        if (unexecuted.putIfAbsent(transfer.id(), transfer) == null)
            startSuspectTimer();

        if (myselfPrimary())
            submitTransaction(transfer);

        else
            channel.send(primary, new ProposeMsg(transfer));
    }

    /**
     * Submits the given transfer to the underlying agreement protocol within a batch of transfers.
     * When the batch reaches a specific threshold, flushes that list of transfers as one single operation.
     * Otherwise, adds the transfer to the current batch being formed, reducing the overhead of consensus.
     * The batch timer is set up when building a new batch.
     * @param transfer the given transfer to order.
     */
    private void submitTransaction(Transfer transfer) {
        currentBatch.add(transfer);
        if (currentBatch.size() == 1)
            batchTimer = runtime.setUpTimer(new BatchTimer(), params.batchTimeout(), TimeUnit.MILLISECONDS);

        if (currentBatch.size() >= params.batchSize())
            submitCurrentBatch();
    }

    /**
     * Proposes the current batch to agreement,
     * clearing it for a new batch to be formed.
     * The batch timer is canceled.
     */
    private void submitCurrentBatch() {
        if (batchTimer != null) {
            batchTimer.cancel(false);
            batchTimer = null;
        }

        if (currentBatch.isEmpty())
            return; // preventing ordering empty batches, ideally not happening ever

        runtime.deliverEvent(new Propose(new ArrayList<>(currentBatch)));
        currentBatch.clear();
    }

    /**
     * When receiving an ordered batch from the Agreement protocol, proceed as follows:
     * - if the sequence number is old, ignore it (already on a posterior phase);
     * - unfold the batch and deliver the transfers in the List order (all the same for each replica),
     * if they are not yet executed and are valid transfers.
     */
    @UponBabelEvent
    public void uponDeliverTransfer(Decided decided) {
        if (decided.sqn() < nextSeqn)
            return;

        List<Transfer> batch = decided.batch().stream().map(m -> (Transfer) m).toList();

        pending.putIfAbsent(decided.sqn(), batch);
        decidePending(); // decide that transfer by in pending (in case holes were detected)
    }

    /**
     * Decide pending operations, those received when the next sequence number were not respected.
     */
    private void decidePending() {
        List<Transfer> next;
        boolean progress = false;
        while ((next = pending.remove(nextSeqn)) != null) {
            for (Transfer t : next) {
                runtime.deliverEvent(new ExecuteTransaction(t));
                unexecuted.remove(t.id());
            }

            nextSeqn++;
            progress = true;
        }

        if (!progress)
            return;

        // progress made so reset the suspect timer for what's is still unexecuted
        stopSuspectTimer();
        if (!unexecuted.isEmpty())
            startSuspectTimer();
    }

    /**
     * Upon a view change from the underlying layer, the replica installs the new view, elects the primary and resets primary failure detection.
     * It discards outdated batch state (if there was one at all) and resubmits all unexecuted requests,
     * either proposing them directly if the replica is the new primary,
     * or by forwarding them to the new primary while monitoring its health.
     */
    @UponBabelEvent
    public void uponViewChanged(ViewChanged viewChanged) {
        currentView = viewChanged.view();
        primary = expectedPrimary(viewChanged.view());
        stopSuspectTimer();
        currentBatch.clear();

        if (myselfPrimary()) // the new primary
            unexecuted.values().forEach(this::submitTransaction);   // re-propose what is still pending

        else {
            unexecuted.values().forEach(t -> channel.send(primary, new ProposeMsg(t)));
            startSuspectTimer();
        }
    }

    // MESSAGES

    /**
     * In case a replica forward the transaction to the primary,
     * the primary submits the transaction as a normal upper layer indication
     */
    @UponMessageIn
    public void uponProposeMsg(ProposeMsg propose, BabelNode sender) {
        LOGGER.debug("Receive propose from replica {}", sender);
        submitTransaction(propose.value());
    }
}

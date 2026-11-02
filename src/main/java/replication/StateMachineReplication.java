package replication;

import agreement.events.Decided;
import agreement.events.Propose;
import agreement.messages.ProposeMsg;
import app.messages.Transfer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

import java.util.*;
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

    private final int batchSize;
    private List<BabelMessage> currentBatch;

    private final Set<String> submitted; // already submitted operations to Agreement protocol
    private long nextSeqn;
    private final List<DecidedTransferBatch> pending; // pending operations to be executed

    private BabelNode primary;

    private Babel runtime;
    private BabelChannel channel;

    private final Predicate<Transfer> validTransfer;

    // TODO: currentBatch operations

    public StateMachineReplication(List<BabelNode> membership, Predicate<Transfer> validTransfer, int batchSize) {
        if (membership.isEmpty())
            throw new IllegalArgumentException("Require a non-empty membership");

        this.batchSize = batchSize;
        this.currentBatch = new LinkedList<>();

        this.submitted = new HashSet<>();
        this.nextSeqn = 1L; // the first seqn conventioned to be executed
        this.pending = new LinkedList<>();

        this.primary = membership.getFirst(); // by convention!

        this.validTransfer = validTransfer;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
    }

    private record DecidedTransferBatch(List<Transfer> ms, long seqn) {}

    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        Transfer transfer = submit.transfer();

        if (submitted.add(transfer.id())) {
            LOGGER.info("Batching transfer {}", transfer.id());
            currentBatch.add(transfer);
        }

        if (currentBatch.size() < batchSize)
            return;

        LOGGER.info("Submitting currentBatch {}", nextSeqn);

        List<BabelMessage> toPropose = new ArrayList<>(currentBatch);
        if (runtime.getMyself().equals(primary))
            runtime.deliverEvent(new Propose(toPropose));
        else
            channel.send(primary, new ProposeMsg(toPropose));

        currentBatch.clear();
    }

    private void deliverPending() {
        while (!pending.isEmpty()) {
            DecidedTransferBatch toDeliver = pending.getFirst();
            List<Transfer> transfers = toDeliver.ms();

            if (toDeliver.seqn() == nextSeqn) {
                for (Transfer transfer : transfers) {
                    LOGGER.info("Catching up on transfer {}", transfer.id());
                    runtime.deliverEvent(new ExecuteTransaction(transfer));
                }

                pending.removeFirst();
                nextSeqn++;

            } else
                break;
        }
    }

    @UponBabelEvent
    public void uponDeliverTransfer(Decided decided) {
        List<Transfer> batch = new ArrayList<>(batchSize);
        for (BabelMessage m : decided.batch()) {
            Transfer transfer = (Transfer) m;

            if (!validTransfer.test(transfer)) {
                LOGGER.warn("Ignoring invalid delivered transfer {} in currentBatch {}", transfer.id(), decided.sqn());
                continue;
            }

            batch.add(transfer);
        }

        if (decided.sqn() == nextSeqn) {
            LOGGER.info("Delivering transfers in currentBatch seqn={}", decided.sqn());
            batch.forEach(transfer -> runtime.deliverEvent(new ExecuteTransaction(transfer)));
            nextSeqn++;

            deliverPending();

        } else {
            LOGGER.warn("Hole detected! Adding currentBatch seqn={} to pending", decided.sqn());
            pending.add(new DecidedTransferBatch(batch, decided.sqn()));
        }
    }
}

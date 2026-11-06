package replication;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import app.messages.Transfer;
import broadcast.events.SelectiveBroadcastRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponBroadcastDeliver;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastConsumerBabelProtocol;
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

public class BroadcastReplicationProtocol implements BroadcastConsumerBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(BroadcastReplicationProtocol.class);

    private final Set<String> submitted = new HashSet<>();
    private final Set<String> executed = new HashSet<>();

    private Babel runtime;

    private final Predicate<Transfer> validTransfer;
    private final BroadcastReplicationProfile profile;

    public BroadcastReplicationProtocol(Predicate<Transfer> validTransfer) {
        this(validTransfer, BroadcastReplicationProfile.NONE);
    }

    public BroadcastReplicationProtocol(
            Predicate<Transfer> validTransfer,
            BroadcastReplicationProfile profile) {
        this.validTransfer = validTransfer;
        this.profile = profile;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
    }

    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        Transfer transfer = submit.transfer();

        if (!executed.contains(transfer.id())
                && submitted.add(transfer.id())) {
            LOGGER.debug("Submitting transfer {}", transfer.id());
            if (profile == BroadcastReplicationProfile.SELECTIVE_SEND) {
                LOGGER.warn("BYZANTINE_ACTION profile=selective_send transfer={}", transfer.id());
                runtime.deliverEvent(new SelectiveBroadcastRequest(transfer, 2));
            } else {
                broadcast(runtime, transfer);
            }
        }
    }

    @UponBroadcastDeliver
    public void uponDeliverTransfer(Transfer transfer, BabelNode origin) {
        if (!validTransfer.test(transfer)) {
            LOGGER.warn("Ignoring invalid broadcast transfer");
            return;
        }

        if (executed.add(transfer.id())) {
            LOGGER.debug("Delivering transfer {}", transfer.id());
            runtime.deliverEvent(new ExecuteTransaction(transfer));
        }
    }

}

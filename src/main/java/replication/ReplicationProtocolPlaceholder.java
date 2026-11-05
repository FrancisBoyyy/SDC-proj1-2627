package replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

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
public class ReplicationProtocolPlaceholder implements BabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(ReplicationProtocolPlaceholder.class);

    private Babel runtime;

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        LOGGER.warn("Using the replication placeholder: transfers are NOT replicated!");
    }

    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        // This logic is evidently incorrect
        runtime.deliverEvent(new ExecuteTransaction(submit.transfer()));
    }
}

package agreement.events;

import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * Request (State Machine Replication -> Agreement Protocol): propose a value
 * (typically a batch of transfers) to be decided in the next consensus
 * instance. Only the current primary (leader) proposes; the other replicas may
 * ignore this request (or use it to detect that the primary is not doing its
 * job).
 *
 * <p>
 * The value is a {@link BabelMessage} so that it can be sent inside the
 * messages of the agreement protocol. Define your own record for batches.
 */
public record Propose(BabelMessage value) implements ProtocolEvent {
}

package agreement.events;

import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

import java.util.List;

/**
 * Request (State Machine Replication -> Agreement Protocol): propose a value
 * (typically a batch of transfers) to be decided in the next consensus
 * instance. Only the current primary (leader) proposes; the other replicas may
 * ignore this request (or use it to detect that the primary is not doing its
 * job).
 */
public record Propose(List<BabelMessage> batch) implements ProtocolEvent {
}

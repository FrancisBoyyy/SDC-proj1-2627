package agreement.events;

import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * Indication (Agreement Protocol -> State Machine Replication): a new view was
 * installed, and {@code primary} is the primary (leader) of that view (primary
 * = membership[view mod n]). The membership itself is static: a view change
 * only replaces the primary.
 */
public record ViewChanged(long view, BabelNode primary) implements ProtocolEvent {
}

package replication.timers;

import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * The suspect timer contains the view that the primary is being suspected.
 * This enables a view change respecting that view number in the underlying protocol.
 */
public record SuspectTimer(long view) implements ProtocolEvent {
}

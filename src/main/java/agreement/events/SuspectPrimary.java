package agreement.events;

import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * Request (State Machine Replication -> Agreement Protocol): the replication
 * protocol suspects that the primary of {@code view} is faulty, for instance
 * because transfers it forwarded to the primary were not ordered in due time.
 * The agreement protocol should start a view change (as described in the
 * lectures), unless the view has already changed in the meantime.
 */
public record SuspectPrimary(long view) implements ProtocolEvent {
}

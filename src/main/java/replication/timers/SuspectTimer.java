package replication.timers;

import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

public record SuspectTimer(long view) implements ProtocolEvent {
}

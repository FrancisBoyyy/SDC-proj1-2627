package replication.timers;

import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

public record FetchTimer(long transferTarget) implements ProtocolEvent {
}

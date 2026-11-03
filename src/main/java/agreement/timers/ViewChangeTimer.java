package agreement.timers;

import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

public record ViewChangeTimer(long view) implements ProtocolEvent {
}

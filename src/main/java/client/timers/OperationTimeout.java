package client.timers;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

public record OperationTimeout(UUID requestId) implements ProtocolEvent {
}

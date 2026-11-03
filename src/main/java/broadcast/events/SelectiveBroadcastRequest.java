package broadcast.events;

import java.util.Objects;

import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/** Test-only request for a Byzantine broadcaster to send SEND to a subset. */
public record SelectiveBroadcastRequest(BabelMessage payload, int recipientCount)
        implements ProtocolEvent {
    public SelectiveBroadcastRequest {
        Objects.requireNonNull(payload, "payload");
        if (recipientCount < 1)
            throw new IllegalArgumentException("recipientCount must be positive");
    }
}

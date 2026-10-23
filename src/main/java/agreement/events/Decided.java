package agreement.events;

import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * Indication (Agreement Protocol -> State Machine Replication): value was
 * decided in consensus instance {@code sqn}. Decisions are indicated in
 * sequence number order (0, 1, 2, ...), without gaps.
 */
public record Decided(long sqn, BabelMessage value) implements ProtocolEvent {
}

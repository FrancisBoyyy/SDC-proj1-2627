package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

/**
 * Sent by the primary of {@code view}: the 2f+1 ViewChange messages it based its decision on, and the set O of
 * PrePrepares (in {@code view}) that every replica must process before accepting new proposals.
 */
@BabelIPMessage(id = 308)
public record CheckpointMsg(long seqn, String digest, BabelNode sender, byte[] sig) implements BabelMessage {
}

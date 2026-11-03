package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

import java.util.List;

/**
 * Sent by the primary of {@code view}: the 2f+1 ViewChange messages it based its decision on, and the set O of
 * PrePrepares (in {@code view}) that every replica must process before accepting new proposals.
 */
@BabelIPMessage(id = 307)
public record NewViewMsg(long view, List<ViewChangeMsg> viewChanges, List<PrePrepareMessage> o) implements BabelMessage {
}

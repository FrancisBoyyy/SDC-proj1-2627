package app.messages;

import java.util.List;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Reply to a {@link HistoryRequest}. At most a limited number of transfers is
 * returned in each reply; {@code size} is the total size of the history of the
 * account, allowing the client to ask for the following positions.
 */
@BabelIPMessage(id = 106)
public record HistoryReply(UUID requestId, String account, int fromIndex, int size, List<Transfer> transfers)
        implements BabelMessage {
}

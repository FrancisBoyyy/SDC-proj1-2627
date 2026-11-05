package app.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Sent by a client to read the executed transfers (outgoing and incoming) of
 * an account, starting at position {@code fromIndex} of the history of that
 * account.
 */
@BabelIPMessage(id = 105)
public record HistoryRequest(UUID requestId, String account, int fromIndex) implements BabelMessage {
}

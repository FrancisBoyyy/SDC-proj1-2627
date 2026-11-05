package app.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Sent by a client to read the current balance (and next sequence number) of
 * an account.
 */
@BabelIPMessage(id = 103)
public record BalanceRequest(UUID requestId, String account) implements BabelMessage {
}

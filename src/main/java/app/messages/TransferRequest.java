package app.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Sent by a client to the replicas to request the execution of a transfer.
 */
@BabelIPMessage(id = 101)
public record TransferRequest(UUID requestId, Transfer transfer) implements BabelMessage {
}

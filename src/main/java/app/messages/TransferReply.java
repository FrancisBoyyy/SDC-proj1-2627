package app.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Sent by a replica to the client once the outcome of a transfer is known.
 *
 * @param status      the name of a {@link app.ledger.TransferStatus}
 *                    (EXECUTED, INVALID, or CONFLICT)
 * @param ledgerIndex the position of the transfer in the local ledger of the
 *                    replica (or -1 if it was not executed). Notice that
 *                    without a total order, different replicas might report
 *                    different positions.
 */
@BabelIPMessage(id = 102)
public record TransferReply(UUID requestId, String account, long seq, String status, long ledgerIndex)
        implements BabelMessage {
}

package app.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Reply to a {@link BalanceRequest}.
 *
 * @param exists  false if the account does not exist (balance and nextSeq are
 *                then meaningless)
 * @param nextSeq the sequence number that the next transfer from this account
 *                must have
 */
@BabelIPMessage(id = 104)
public record BalanceReply(UUID requestId, String account, boolean exists, long balance, long nextSeq)
        implements BabelMessage {
}

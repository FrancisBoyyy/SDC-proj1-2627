package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Requests decision proofs from {@code fromSeqn} through {@code toSeqn}. A zero {@code toSeqn}
 * asks the peer for a bounded chunk through its latest executed sequence number.
 */
@BabelIPMessage(id = 409)
public record FetchMsg(long fromSeqn, long toSeqn) implements BabelMessage {}

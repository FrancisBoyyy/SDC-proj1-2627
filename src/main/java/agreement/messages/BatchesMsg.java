package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import java.util.List;

/**
 * Catch-up reply: signed decision proofs for the contiguous seqns starting at
 * {@code fromSeqn}. {@code latestSeqn} tells the receiver whether another
 * bounded fetch is needed.
 */
@BabelIPMessage(id = 410)
public record BatchesMsg(long fromSeqn, long latestSeqn, List<DecisionProof> decisions) implements BabelMessage {}

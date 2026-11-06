package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

import java.util.List;

/**
 * State transfer reply: the decided batches for the contiguous seqns
 * {@code fromSeqn, fromSeqn + 1, ...}, in order.
 */
@BabelIPMessage(id = 310)
public record BatchesMsg(long fromSeqn, List<List<BabelMessage>> batches) implements BabelMessage {}

package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

import java.util.List;

@BabelIPMessage(id = 310)
public record BatchesMsg(long fromSeqn, List<BatchEntry> batches) implements BabelMessage {}
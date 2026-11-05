package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

import java.util.List;

@BabelIPMessage(id = 311)
public record BatchEntry(long seqn, List<BabelMessage> batch) implements BabelMessage {}
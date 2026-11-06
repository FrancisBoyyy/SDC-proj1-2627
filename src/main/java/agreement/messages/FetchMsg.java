package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

@BabelIPMessage(id = 309)
public record FetchMsg(long fromSeqn, long toSeqn) implements BabelMessage {}

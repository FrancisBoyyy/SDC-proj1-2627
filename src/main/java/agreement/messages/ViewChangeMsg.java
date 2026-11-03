package agreement.messages;

import agreement.PBFTState;
import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

@BabelIPMessage(id = 304)
public record ViewChangeMsg(long newView, PreparedProof proof) implements BabelMessage {
}

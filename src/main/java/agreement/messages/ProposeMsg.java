package agreement.messages;

import app.messages.Transfer;
import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

@BabelIPMessage(id = 403)
public record ProposeMsg(Transfer value) implements BabelMessage {
}

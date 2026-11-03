package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

import java.util.List;

/**
 * Signed by {@code sender} so that it can be forwarded inside a {@link NewViewMsg}.
 */
@BabelIPMessage(id = 304)
public record ViewChangeMsg(long newView, List<PreparedProof> proofs, BabelNode sender, byte[] sig) implements BabelMessage {
}

package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

import java.util.List;

@BabelIPMessage(id = 306)
public record PreparedProof(SignedProof preprepare, List<SignedProof> prepares) implements BabelMessage {}


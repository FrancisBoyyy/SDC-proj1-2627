package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

import java.util.List;

/**
@BabelIPMessage(id = 304)
public record ViewChangeMsg(long newView, List<PreparedProof> proofs, BabelNode sender, byte[] sig) implements BabelMessage {
}
 */

@BabelIPMessage(id = 304)
public record ViewChangeMsg(long newView, long stableSeqn, List<CheckpointMsg> checkpointProof,
                            List<PreparedProof> proofs, BabelNode sender, byte[] sig) implements BabelMessage {
}

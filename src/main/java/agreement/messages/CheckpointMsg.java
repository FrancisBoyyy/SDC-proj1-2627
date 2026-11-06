package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

/**
 * Signed by replica {@code sender} after executing the request with sequence number {@code seqn}
 * (a multiple of the checkpoint interval); {@code digest} covers the decided history up to {@code seqn}.
 * 2f+1 matching messages make the checkpoint stable and are its proof.
 */
@BabelIPMessage(id = 408)
public record CheckpointMsg(long seqn, String digest, BabelNode sender, byte[] sig) implements BabelMessage {
}

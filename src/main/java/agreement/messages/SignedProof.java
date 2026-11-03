package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

@BabelIPMessage(id = 305)
public record SignedProof(long view, long seqn, String digest, BabelNode sender, byte[] signature) implements BabelMessage {
    public SignedProof(PrePrepareMessage msg, BabelNode from) {
        this(msg.view(), msg.seqn(), msg.hash(), from, msg.sig());
    }

    public SignedProof(PrepareMessage msg, BabelNode from) {
        this(msg.view(), msg.seqn(), msg.hash(), from, msg.sig());
    }
}

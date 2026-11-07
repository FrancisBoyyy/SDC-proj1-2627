package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

import java.util.List;

/** A batch and the signed PBFT decision evidence needed to safely catch up a replica. */
@BabelIPMessage(id = 412)
public record DecisionProof(
        long seqn,
        List<BabelMessage> batch,
        PrePrepareMessage prePrepare,
        List<ProofEntry> commits
) implements BabelMessage {}

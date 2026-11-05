package agreement.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

import java.util.List;

/**
 * Proof that (view, seqn, digest) prepared at a replica:
 * - the primary's signed PrePrepare
 * - the batch it refers to, so a new primary can re-propose it
 * - the signed Prepares of 2f distinct backups.
 */
@BabelIPMessage(id = 306)
public record PreparedProof(List<BabelMessage> batch, ProofEntry preprepare, List<ProofEntry> prepares) implements BabelMessage {

}

package agreement;

import agreement.messages.*;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import utils.Crypto;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PBFTState {

    private List<BabelMessage> batch;
    private String digest;

    private boolean preprepared;
    private boolean prepared;
    private boolean commited;

    private ProofEntry signedPrePrepare;
    private final Map<BabelNode, ProofEntry> prepares;
    private final Map<BabelNode, CommitMessage> commits;

    public PBFTState() {
        this.batch = null;
        this.digest = null;

        this.preprepared = false;
        this.prepared = false;
        this.commited = false;

        this.signedPrePrepare = null;
        this.prepares = new HashMap<>();
        this.commits = new HashMap<>();
    }

    // aux methods

    public static String batchDigest(List<BabelMessage> batch) {
        String batchString = Arrays.toString(batch.toArray());
        byte[] hashBytes = Crypto.digest(batchString.getBytes());
        return Crypto.toHex(hashBytes);
    }

    // getters and setters

    public List<BabelMessage> getBatch() {
        return batch;
    }

    public String getDigest() {
        return digest;
    }

    /**
     * The replica can accept a PrePrepare with the given hash if it did not
     * previously accept a PrePrepare with a different hash.
     */
    public boolean canAcceptPrePrepare(String hash) {
        return !preprepared || hash.equals(digest);
    }

    public void setPrePrepared(PrePrepareMessage msg, BabelNode primary) {
        preprepared = true;
        this.batch =msg.ms();
        this.digest = batchDigest(msg.ms());

        signedPrePrepare = new ProofEntry(msg, primary);
    }

    public void setPrepared() {
        prepared = true;
    }

    public void setCommited() {
        commited = true;
    }

    /**
     * Registers the Prepare received from the given process.
     * This operation occurs at most once per process.
     */
    public void registerPrepare(BabelNode from, PrepareMessage msg) {
        prepares.putIfAbsent(from,
                new ProofEntry(msg, from));
    }

    /**
     * Only computes the size if it is in conditions to do so.
     * If it is already on a posterior protocol phase, ignores it by returning 0
     */
    public int getPrepareSize() {
        if (!preprepared || prepared)
            return 0;

        int count = 1; // already include the primary pre-prepare
        for (ProofEntry proof : prepares.values()) {
            if (proof.digest().equals(digest))
                count++;
        }

        return count;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public PreparedProof getProof() {
        return new PreparedProof(
                batch,
                signedPrePrepare,
                // consider only prepares with the same digest as the accepted PrePrepare
                prepares.values().stream().filter(p -> p.digest().equals(digest)).toList()
        );
    }

    public void registerCommit(BabelNode from, CommitMessage msg) {
        commits.putIfAbsent(from, msg);
    }

    /**
     * Only computes the size if it is in conditions to do so.
     * If it is already on a posterior protocol phase, ignores it by returning 0
     */
    public int getCommitSize() {
        if (!prepared || commited)
            return 0;

        int count = 0;
        for (CommitMessage msg : commits.values()) {
            if (msg.hash().equals(digest))
                count++;
        }

        return count;
    }
}

package agreement;

import agreement.messages.CommitMessage;
import agreement.messages.PrepareMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import utils.Crypto;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PBFTState {

    private List<BabelMessage> batch;

    private boolean preprepared;
    private boolean prepared;
    private boolean commited;

    private final Map<BabelNode, PrepareMessage> prepares;
    private final Map<BabelNode, CommitMessage> commits;

    public PBFTState() {
        this.batch = null;

        this.preprepared = false;
        this.prepared = false;
        this.commited = false;

        this.prepares = new HashMap<>();
        this.commits = new HashMap<>();
    }

    // aux methods

    public static String operationDigest(List<BabelMessage> batch) {
        String batchString = Arrays.toString(batch.toArray());
        byte[] hashBytes = Crypto.digest(batchString.getBytes());
        return Crypto.toHex(hashBytes);
    }

    // getters and setters

    public List<BabelMessage> getBatch() {
        return batch;
    }

    /**
     * The replica can accept a PrePrepare with the given hash if it did not
     * previously accept a PrePrepare with a different hash.
     */
    public boolean canAcceptPrePrepare(String hash) {
        return !preprepared || hash.equals(operationDigest(batch));
    }

    public void setPrePrepared(List<BabelMessage> op) {
        preprepared = true;
        this.batch =op;
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
        prepares.putIfAbsent(from, msg);
    }

    /**
     * Only computes the size if it is in conditions to do so.
     * If it is already on a posterior protocol phase, ignores it by returning 0
     */
    public int getPrepareSize() {
        if (!preprepared || prepared)
            return 0;

        int count = 1; // already include the primary pre-prepare
        for (PrepareMessage msg : prepares.values()) {
            if (msg.hash().equals(operationDigest(batch)))
                count++;
        }

        return count;
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
            if (msg.hash().equals(operationDigest(batch)))
                count++;
        }

        return count;
    }
}

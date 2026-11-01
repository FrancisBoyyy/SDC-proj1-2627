package agreement;

import agreement.messages.CommitMessage;
import agreement.messages.PrepareMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import utils.Crypto;

import java.util.HashMap;
import java.util.Map;

public class PBFTState {

    private BabelMessage op;

    private boolean preprepared;
    private boolean prepared;
    private boolean commited;

    private final Map<BabelNode, PrepareMessage> prepares;
    private final Map<BabelNode, CommitMessage> commits;

    public PBFTState() {
        this.op = null;

        this.preprepared = false;
        this.prepared = false;
        this.commited = false;

        this.prepares = new HashMap<>();
        this.commits = new HashMap<>();
    }

    // aux methods

    public static String operationDigest(BabelMessage operation) {
        byte[] hashBytes = Crypto.digest(operation.toString().getBytes());
        return Crypto.toHex(hashBytes);
    }

    // getters and setters

    /**
     * The replica can accept a PrePrepare with the given hash if it did not
     * previously accept a PrePrepare with a different hash.
     */
    public boolean canAcceptPrePrepare(String hash) {
        return !preprepared || hash.equals(operationDigest(op));
    }

    public void setPrePrepared(BabelMessage op) {
        preprepared = true;
        this.op =op;
    }

    public boolean isPreparedState() {
        return prepared;
    }

    public void setPrepared() {
        prepared = true;
    }

    public boolean isCommitedState() {
        return commited;
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

    public int getPrepareSize() {
        if (!preprepared)
            return 0;

        int count = 1; // already include the primary pre-prepare
        for (PrepareMessage msg : prepares.values()) {
            if (msg.hash().equals(operationDigest(op)))
                count++;
        }

        return count;
    }

    public void registerCommit(BabelNode from, CommitMessage msg) {
        commits.putIfAbsent(from, msg);
    }

    public int getCommitSize() {
        if (!prepared)
            return 0;

        int count = 0;
        for (CommitMessage msg : commits.values()) {
            if (msg.hash().equals(operationDigest(op)))
                count++;
        }

        return count;
    }
}

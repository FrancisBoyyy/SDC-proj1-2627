package client;

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import pt.unl.fct.di.novasys.babel2.core.BabelNode;

/**
 * Collects the replies of different replicas to the same request, and decides
 * when the client can trust a reply: when {@code quorum} different replicas
 * sent matching replies (by default f+1, so that at least one correct replica
 * vouches for the reply).
 *
 * <p>
 * Only the first reply of each replica counts (a Byzantine replica cannot vote
 * twice).
 *
 * @param <T> the type of the replies
 */
public final class ReplyQuorum<T> {
    private final int quorum;
    private final Map<BabelNode, String> voted;
    private final Map<String, Integer> votes;

    public ReplyQuorum(int quorum) {
        if (quorum < 1) {
            throw new IllegalArgumentException("Quorum must be at least 1");
        }
        this.quorum = quorum;
        this.voted = new HashMap<>();
        this.votes = new HashMap<>();
    }

    /**
     * Registers the reply of a replica.
     *
     * @param matchKey the part of the reply that must match among replicas
     * @return the reply if this reply completed a quorum of matching replies,
     *         null otherwise
     */
    public @Nullable T add(BabelNode replica, String matchKey, T reply) {
        if (voted.putIfAbsent(replica, matchKey) != null) {
            return null;
        }
        int count = votes.merge(matchKey, 1, Integer::sum);
        return count == quorum ? reply : null;
    }

    public int replies() {
        return voted.size();
    }

    /**
     * @return true if the replies received so far disagree (some replicas might
     *         be faulty, or replicas might not be consistent)
     */
    public boolean divergent() {
        return votes.size() > 1;
    }
}

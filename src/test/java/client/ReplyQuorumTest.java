package client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import pt.unl.fct.di.novasys.babel2.channels.PublicKeyBabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

class ReplyQuorumTest {

    private final List<BabelNode> replicas = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        var generator = KeyPairGenerator.getInstance("Ed25519");
        for (int i = 0; i < 4; i++) {
            replicas.add(new PublicKeyBabelNode(generator.generateKeyPair().getPublic()));
        }
    }

    @Test
    void acceptsAfterQuorumOfMatchingReplies() {
        ReplyQuorum<String> quorum = new ReplyQuorum<>(2);
        assertNull(quorum.add(replicas.get(0), "A", "a0"));
        assertNull(quorum.add(replicas.get(1), "B", "b1"));
        assertTrue(quorum.divergent());
        assertEquals("a2", quorum.add(replicas.get(2), "A", "a2"));
        // Only reported once
        assertNull(quorum.add(replicas.get(3), "A", "a3"));
        assertEquals(4, quorum.replies());
    }

    @Test
    void aReplicaOnlyVotesOnce() {
        ReplyQuorum<String> quorum = new ReplyQuorum<>(2);
        assertNull(quorum.add(replicas.get(0), "A", "a"));
        assertNull(quorum.add(replicas.get(0), "A", "a"));
        assertNull(quorum.add(replicas.get(0), "B", "b"));
        assertFalse(quorum.divergent());
        assertEquals(1, quorum.replies());
    }
}

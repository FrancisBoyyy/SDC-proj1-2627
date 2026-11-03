package broadcast;

import broadcast.messages.SendBroadcastMessage;
import org.jspecify.annotations.Nullable;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class BroadcastState {
    // Retain our emitted messages for replay after connection is established
    @Nullable SendBroadcastMessage send;
    @Nullable SendBroadcastMessage echo;
    @Nullable SendBroadcastMessage ready;

    // Empty means normal dissemination to every peer. Otherwise this SEND
    // message is intentionally restricted to these recipients.
    final Set<BabelNode> sendTargets = new HashSet<>();

    boolean delivered;

    final Map<BabelNode, SendBroadcastMessage> echoes = new HashMap<>();
    final Map<BabelNode, SendBroadcastMessage> readies = new HashMap<>();

    static long count (
            Map<BabelNode, SendBroadcastMessage> votes,
            SendBroadcastMessage candidate
    ) {
        return votes.values().stream()
                .filter(candidate::equals)
                .count();
    }
}

package membership;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

import membership.timers.ReconnectTimer;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipProviderBabelProtocol;

/**
 * Static membership: every replica knows all other replicas (from the trust
 * store) and keeps a connection to each one of them, notifying the protocols
 * above (NeighbourUp/NeighbourDown) when connections go up or down.
 *
 * <p>
 * To avoid both sides of a connection trying to connect simultaneously, each
 * replica only initiates connections to the replicas that come before it in
 * the (sorted) membership; the other replicas connect to it. Connection
 * attempts are repeated periodically while the connection is down (replicas
 * might be started in any order).
 */
public class StaticMembershipProtocol implements MembershipProviderBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(StaticMembershipProtocol.class);

    private final List<BabelNode> membership;
    private final long reconnectInterval;
    private final Set<BabelNode> connected;

    private Babel runtime;
    private BabelChannel channel;
    private List<BabelNode> toConnect;

    public StaticMembershipProtocol(List<BabelNode> membership, long reconnectInterval) {
        this.membership = List.copyOf(membership);
        this.reconnectInterval = reconnectInterval;
        this.connected = new HashSet<>();
        this.toConnect = List.of();
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
        int myIndex = membership.indexOf(runtime.getMyself());
        if (myIndex < 0) {
            throw new IllegalStateException("This process is not part of the membership");
        }
        this.toConnect = membership.subList(0, myIndex);
        for (var node : toConnect) {
            channel.connect(node);
        }
        runtime.setUpTimer(new ReconnectTimer(), reconnectInterval, reconnectInterval, TimeUnit.MILLISECONDS);
    }

    @UponBabelEvent
    public void uponReconnectTimer(ReconnectTimer timer) {
        for (var node : toConnect) {
            if (!connected.contains(node)) {
                LOGGER.debug("Trying to connect to {}", node);
                channel.connect(node);
            }
        }
    }

    @Override
    public void connectionUp(BabelNode peer) {
        if (!membership.contains(peer)) {
            return;
        }
        if (connected.add(peer)) {
            LOGGER.info("Replica {} is up", peer);
            this.neighbourUp(runtime, peer);
        }
    }

    @Override
    public void connectionDown(BabelNode peer, @Nullable Throwable cause) {
        handleDown(peer);
    }

    @Override
    public void connectionFailed(BabelNode peer, @Nullable Throwable cause) {
        handleDown(peer);
    }

    private void handleDown(BabelNode peer) {
        if (connected.remove(peer)) {
            LOGGER.info("Replica {} is down", peer);
            this.neighbourDown(runtime, peer);
        }
    }
}

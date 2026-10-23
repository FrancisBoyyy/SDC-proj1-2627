import java.security.cert.CertificateException;

import app.PaymentService;
import app.ledger.Ledger;
import broadcast.CrashFaultReliableBroadcastProtocol;
import membership.StaticMembershipProtocol;
import pt.unl.fct.di.novasys.babel2.channels.DefaultNodeStore;
import pt.unl.fct.di.novasys.babel2.channels.IPBabelChannel.AuthMode;
import pt.unl.fct.di.novasys.babel2.channels.tcp_tls.TCPBabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.utils.BabelIpUtils;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.NettyExecutorProtocolHolderFactory;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastConsumerBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastConsumerExtensionFactory;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastProviderBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastProviderExtensionFactory;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipConsumerBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipConsumerExtensionFactory;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipProviderBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipProviderExtensionFactory;
import pt.unl.fct.di.novasys.babel2.utils.BabelPropertiesReader;
import replication.ReplicationProtocolPlaceholder;
import utils.BabelIPComparator;
import utils.Crypto;
import utils.SystemConfiguration;

// Entry point of a replica of the payment service. Run it with:
// java -Dio.netty.transport.noNative=true -jar target/project1-1.0.jar crypto_name=node1

final String DEFAULT_CONFIG_FILE = "babel.conf";

void main(String[] args) throws IOException, KeyStoreException,
        NoSuchAlgorithmException, CertificateException, UnrecoverableEntryException {

    // Reads the configuration file. Any property can be overridden in the command
    // line (e.g., crypto_name=node2 initial_balance=500).
    var properties = BabelPropertiesReader.loadConfig(Paths.get(DEFAULT_CONFIG_FILE), args);

    // The static configuration of the system: membership (from the trust store),
    // addresses of the replicas, and accounts of the initial state.
    var config = SystemConfiguration.load(properties);
    var keyPair = Crypto.getKeyPair(properties);
    var myName = properties.getProperty(Crypto.CRYPTO_NAME_KEY);
    var membership = config.getReplicas();

    // A pool of netty threads shared by all protocols. The protocol handler
    // atomic execution is guaranteed in any executor policy (i.e., the handlers of
    // a protocol never execute concurrently).
    int threads = Integer.parseInt(properties.getProperty("threads", "4"));
    var eventLoop = BabelIpUtils.createLoopGroup(threads);
    var nettyHolderFactory = new NettyExecutorProtocolHolderFactory(
            Integer.parseInt(properties.getProperty("event_queue_size", "65536")), eventLoop);

    // The NodeStore is where the framework stores information on how to contact
    // each node (IP address + port). We fill it with the (static) addresses of
    // all replicas, instead of using a discovery protocol.
    var nodeStore = new DefaultNodeStore(membership.size(), BabelIPComparator.INSTANCE);
    config.registerAddresses(nodeStore, false);

    // Channel used for the communication among replicas. We only accept
    // connections from replicas, and each connection is authenticated: the other
    // side must prove (with a challenge) that it owns the private key of its
    // identity. Hence, the sender of every message is authenticated
    // (authenticated perfect point-to-point links). MIXED_AUTH allows you to also
    // send messages signed by the channel (SignedBabelMessage), although we
    // recommend you to sign explicitly whatever must be verified by third parties
    // (see utils.Crypto).
    var replicaChannel = TCPBabelChannel.builder()
            .setEventLoopGroup(eventLoop)
            .setBindingAddress(config.getReplicaAddress(myName))
            .setPermittedNodes(Set.copyOf(membership))
            .verifyIdentity()
            .setAuthMode(AuthMode.MIXED_AUTH)
            .setNumberOfConnections(membership.size())
            .setNumberOfMessages(16);

    // Channel used to interact with clients. Any client can connect, but it must
    // also prove its identity.
    var clientChannel = TCPBabelChannel.builder()
            .setEventLoopGroup(eventLoop)
            .setBindingAddress(config.getClientAddress(myName))
            .verifyIdentity()
            .setAuthMode(AuthMode.MIXED_AUTH)
            .setNumberOfConnections(64)
            .setNumberOfMessages(8);

    // The application (the same for all the variants of the project)
    var ledger = new Ledger(config.getAccountOwners(), config.getClientKeys(), config.getInitialBalance());
    var paymentService = new PaymentService(ledger,
            Long.parseLong(properties.getProperty("digest_interval", "5000")));

    // The replication and dissemination/agreement protocols: replace these by
    // your own protocols.
    var replicationProtocol = new ReplicationProtocolPlaceholder();
    var broadcastProtocol = new CrashFaultReliableBroadcastProtocol(membership);
    var membershipProtocol = new StaticMembershipProtocol(membership,
            Long.parseLong(properties.getProperty("reconnect_interval", "1000")));

    System.err.println("Starting replica " + myName + " (n=" + config.getN() + ", f=" + config.getF()
            + ", replicas at " + config.getReplicaAddress(myName) + ", clients at "
            + config.getClientAddress(myName) + ")");

    Babel.builder(4, 4, 2, 8, 16)
            .setNodeStore(nodeStore)
            // Extensions that provide optional functionality to Babel (membership and
            // broadcast). Each extension is implemented by one protocol.
            .registerExtension(MembershipConsumerBabelProtocol.class, new MembershipConsumerExtensionFactory())
            .registerExtension(MembershipProviderBabelProtocol.class, new MembershipProviderExtensionFactory())
            .registerExtension(BroadcastConsumerBabelProtocol.class, new BroadcastConsumerExtensionFactory())
            .registerExtension(BroadcastProviderBabelProtocol.class, new BroadcastProviderExtensionFactory())
            .registerProtocol(paymentService, nettyHolderFactory, clientChannel)
            .registerProtocol(replicationProtocol, nettyHolderFactory, replicaChannel)
            .registerProtocol(broadcastProtocol, nettyHolderFactory, replicaChannel)
            .registerProtocol(membershipProtocol, nettyHolderFactory, replicaChannel)
            .setKeyPair(keyPair)
            .build()
            .init();
}

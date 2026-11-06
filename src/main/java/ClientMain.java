import java.security.cert.CertificateException;

import client.AutomatedClient;
import client.ClientParameters;
import client.InteractiveClient;
import pt.unl.fct.di.novasys.babel2.channels.DefaultNodeStore;
import pt.unl.fct.di.novasys.babel2.channels.IPBabelChannel.AuthMode;
import pt.unl.fct.di.novasys.babel2.channels.tcp_tls.TCPBabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.utils.BabelIpUtils;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.NettyExecutorProtocolHolderFactory;
import pt.unl.fct.di.novasys.babel2.ssl.CertGenerator;
import pt.unl.fct.di.novasys.babel2.utils.BabelPropertiesReader;
import utils.BabelIPComparator;
import utils.Crypto;
import utils.SystemConfiguration;

// Entry point of the client process. Run it with:
// java -Dio.netty.transport.noNative=true -cp target/project1-1.0.jar ClientMain [client_mode=interactive]

final String DEFAULT_CONFIG_FILE = "babel.conf";

void main(String[] args) throws IOException, KeyStoreException, NoSuchAlgorithmException,
        CertificateException, UnrecoverableEntryException {

    // Clients read the same configuration file as the replicas (and any property
    // can be overridden in the command line, e.g., num_clients=20).
    var properties = BabelPropertiesReader.loadConfig(Paths.get(DEFAULT_CONFIG_FILE), args);
    var config = SystemConfiguration.load(properties);
    var replicas = config.getReplicas();

    // f+1 matching replies guarantee that at least one correct replica vouches for
    // the reply.
    int replyQuorum = Integer.parseInt(properties.getProperty("reply_quorum", String.valueOf(config.getF() + 1)));
    if (replyQuorum < 1 || replyQuorum > config.getN()) {
        System.err.println("reply_quorum must be between 1 and the number of replicas (" + config.getN() + ")");
        System.exit(1);
    }

    // The client is a single protocol, executed by a (small) pool of netty threads.
    var eventLoop = BabelIpUtils.createLoopGroup(Integer.parseInt(properties.getProperty("client_threads", "2")));
    var nettyHolderFactory = new NettyExecutorProtocolHolderFactory(65536, eventLoop);

    // Clients know the (client facing) address of every replica.
    var nodeStore = new DefaultNodeStore(replicas.size(), BabelIPComparator.INSTANCE);
    config.registerAddresses(nodeStore, true);

    // The identity of the client process (used to authenticate the connections to
    // the replicas) is a fresh key pair. Notice that this is NOT the key used to
    // sign transfers: each (logical) client signs its transfers with its own key
    // (from the clients key store).
    var keyPair = CertGenerator.generateIdentityKeyPair();

    // The channel used to talk with the replicas. Clients only connect to replicas,
    // and the replicas must prove their identity (so the sender of every reply is
    // authenticated, which is essential to count matching replies).
    var channel = TCPBabelChannel.builder()
            .setEventLoopGroup(eventLoop)
            .setPermittedNodes(Set.copyOf(replicas))
            .verifyIdentity()
            .setAuthMode(AuthMode.MIXED_AUTH)
            .setNumberOfConnections(replicas.size())
            .setNumberOfMessages(8);

    // The keys of all (logical) clients, used to sign their transfers
    var clientKeys = Crypto.getClientPrivateKeys(properties);
    long operationTimeout = Long.parseLong(properties.getProperty("operation_timeout", "5000"));

    // The interactive client is used for testing, the automated one for experiments
    BabelProtocol client;
    if ("interactive".equals(properties.getProperty("client_mode", "automated"))) {
        Map<BabelNode, String> names = new HashMap<>();
        replicas.forEach(r -> names.put(r, config.getReplicaName(r)));
        client = new InteractiveClient(replicas, names, clientKeys, config.getAccountOwners(), replyQuorum,
                operationTimeout);
    } else {
        client = new AutomatedClient(replicas, clientKeys, config.getAccountOwners(),
                ClientParameters.fromProperties(properties), replyQuorum);
    }

    Babel.builder(1, 0, 1, 8, 8)
            .setNodeStore(nodeStore)
            .registerProtocol(client, nettyHolderFactory, channel)
            .setKeyPair(keyPair)
            .build()
            .init();
}

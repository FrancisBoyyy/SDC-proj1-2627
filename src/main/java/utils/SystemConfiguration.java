package utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import pt.unl.fct.di.novasys.babel2.channels.IPBabelNodeComponent;
import pt.unl.fct.di.novasys.babel2.channels.NodeStore;
import pt.unl.fct.di.novasys.babel2.channels.PublicKeyBabelNode;
import pt.unl.fct.di.novasys.babel2.channels.tcp_tls.TCPBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

/**
 * Static configuration of the system, shared by replicas and clients.
 *
 * <p>
 * The membership (set of replicas) is static and given by the certificates in
 * the replicas trust store. The address of each replica is read from the
 * configuration file (properties {@code <alias>.replica_address} and
 * {@code <alias>.client_address}). We do not rely on the local network
 * discovery protocol used in the first lab, because it requires multicast,
 * which is not available everywhere (it fails on most laptops with VPNs).
 *
 * <p>
 * The initial state of the ledger (genesis) is also defined here: every client
 * in the clients trust store owns an account with its own name, and additional
 * shared accounts (owned by several clients) are defined by the
 * {@code shared_accounts} property, with the format
 * {@code name:owner1,owner2;name2:owner3,owner4}.
 */
public final class SystemConfiguration {
    public static final String REPLICA_ADDRESS_SUFFIX = ".replica_address";
    public static final String CLIENT_ADDRESS_SUFFIX = ".client_address";
    public static final String INITIAL_BALANCE_KEY = "initial_balance";
    public static final String SHARED_ACCOUNTS_KEY = "shared_accounts";

    // The TCP channel only uses the addresses registered with its own name.
    private static final String TCP_CHANNEL_NAME = TCPBabelChannel.class.getSimpleName();

    private final List<String> replicaAliases;
    private final Map<String, BabelNode> replicas;
    private final Map<BabelNode, String> replicaNames;
    private final Map<String, InetSocketAddress> replicaAddresses;
    private final Map<String, InetSocketAddress> clientAddresses;
    private final Map<String, PublicKey> clientKeys;
    private final Map<String, Set<String>> accountOwners;
    private final long initialBalance;

    private SystemConfiguration(Map<String, PublicKey> replicaKeys, Map<String, PublicKey> clientKeys,
            Properties props) {
        this.replicaAliases = List.copyOf(replicaKeys.keySet());
        this.replicas = new LinkedHashMap<>();
        this.replicaNames = new LinkedHashMap<>();
        this.replicaAddresses = new LinkedHashMap<>();
        this.clientAddresses = new LinkedHashMap<>();
        for (var entry : replicaKeys.entrySet()) {
            var node = new PublicKeyBabelNode(entry.getValue());
            replicas.put(entry.getKey(), node);
            replicaNames.put(node, entry.getKey());
            replicaAddresses.put(entry.getKey(),
                    parseAddress(required(props, entry.getKey() + REPLICA_ADDRESS_SUFFIX)));
            clientAddresses.put(entry.getKey(),
                    parseAddress(required(props, entry.getKey() + CLIENT_ADDRESS_SUFFIX)));
        }
        this.clientKeys = Map.copyOf(clientKeys);
        this.initialBalance = Long.parseLong(props.getProperty(INITIAL_BALANCE_KEY, "1000000"));
        this.accountOwners = new LinkedHashMap<>();
        for (String client : clientKeys.keySet()) {
            accountOwners.put(client, Set.of(client));
        }
        for (var shared : parseSharedAccounts(props.getProperty(SHARED_ACCOUNTS_KEY, "")).entrySet()) {
            for (String owner : shared.getValue()) {
                if (!clientKeys.containsKey(owner)) {
                    throw new IllegalArgumentException(
                            "Shared account " + shared.getKey() + " has an unknown owner: " + owner);
                }
            }
            if (accountOwners.putIfAbsent(shared.getKey(), shared.getValue()) != null) {
                throw new IllegalArgumentException("Duplicated account name: " + shared.getKey());
            }
        }
    }

    public static SystemConfiguration load(Properties props)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        var replicaKeys = Crypto.getPublicKeys(Crypto.getTruststore(props));
        var clientKeys = Crypto.getClientPublicKeys(props);
        return new SystemConfiguration(replicaKeys, clientKeys, props);
    }

    /**
     * Registers the address of every replica in the NodeStore, so that the TCP
     * channel knows where to connect.
     *
     * @param store    the NodeStore of this process
     * @param toClient if true registers the client facing addresses of the
     *                 replicas (used by clients), otherwise registers the
     *                 addresses used for the communication among replicas.
     */
    public void registerAddresses(NodeStore store, boolean toClient) {
        for (String alias : replicaAliases) {
            var address = Objects.requireNonNull(toClient ? clientAddresses.get(alias) : replicaAddresses.get(alias));
            store.addComponent(getReplica(alias), new IPBabelNodeComponent(TCP_CHANNEL_NAME, address));
        }
    }

    /**
     * Replicas, sorted by alias (node1, node2, ...). Every process sees the same
     * order.
     */
    public List<BabelNode> getReplicas() {
        List<BabelNode> list = new ArrayList<>(replicaAliases.size());
        for (String alias : replicaAliases) {
            list.add(getReplica(alias));
        }
        return List.copyOf(list);
    }

    public BabelNode getReplica(String alias) {
        var node = replicas.get(alias);
        if (node == null) {
            throw new IllegalArgumentException("Unknown replica " + alias);
        }
        return node;
    }

    public String getReplicaName(BabelNode node) {
        return replicaNames.getOrDefault(node, "unknown");
    }

    public InetSocketAddress getReplicaAddress(String alias) {
        return Objects.requireNonNull(replicaAddresses.get(alias), "Unknown replica " + alias);
    }

    public InetSocketAddress getClientAddress(String alias) {
        return Objects.requireNonNull(clientAddresses.get(alias), "Unknown replica " + alias);
    }

    /**
     * @return the number of replicas (n)
     */
    public int getN() {
        return replicaAliases.size();
    }

    /**
     * @return the maximum number of Byzantine replicas tolerated (n >= 3f + 1)
     */
    public int getF() {
        return (getN() - 1) / 3;
    }

    public Map<String, PublicKey> getClientKeys() {
        return clientKeys;
    }

    /**
     * @return the accounts of the genesis state (account name -> owners)
     */
    public Map<String, Set<String>> getAccountOwners() {
        return Map.copyOf(accountOwners);
    }

    public long getInitialBalance() {
        return initialBalance;
    }

    static Map<String, Set<String>> parseSharedAccounts(String value) {
        Map<String, Set<String>> accounts = new LinkedHashMap<>();
        for (String account : value.split(";")) {
            account = account.strip();
            if (account.isEmpty()) {
                continue;
            }
            String[] parts = account.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid shared account definition: " + account);
            }
            Set<String> owners = new LinkedHashSet<>();
            for (String owner : parts[1].split(",")) {
                if (!owner.isBlank()) {
                    owners.add(owner.strip());
                }
            }
            if (owners.isEmpty()) {
                throw new IllegalArgumentException("Shared account without owners: " + account);
            }
            accounts.put(parts[0].strip(), Set.copyOf(owners));
        }
        return accounts;
    }

    static InetSocketAddress parseAddress(String value) {
        int idx = value.lastIndexOf(':');
        if (idx <= 0) {
            throw new IllegalArgumentException("Invalid address (expected host:port): " + value);
        }
        return new InetSocketAddress(value.substring(0, idx).strip(),
                Integer.parseInt(value.substring(idx + 1).strip()));
    }

    private static String required(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing mandatory configuration: " + key);
        }
        return value;
    }
}

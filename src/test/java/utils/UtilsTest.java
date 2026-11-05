package utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;

class UtilsTest {

    @Test
    void aliasesAreSortedNumerically() {
        List<String> aliases = new ArrayList<>(List.of("node10", "node2", "node1", "client3", "node11"));
        aliases.sort(AliasComparator.INSTANCE);
        assertEquals(List.of("client3", "node1", "node2", "node10", "node11"), aliases);
    }

    @Test
    void parsesSharedAccounts() {
        Map<String, Set<String>> shared = SystemConfiguration
                .parseSharedAccounts("shared1:client1,client2; shared2 : client3 ,client4,client5;");
        assertEquals(Set.of("client1", "client2"), shared.get("shared1"));
        assertEquals(Set.of("client3", "client4", "client5"), shared.get("shared2"));
        assertEquals(Map.of(), SystemConfiguration.parseSharedAccounts(""));
        assertThrows(IllegalArgumentException.class, () -> SystemConfiguration.parseSharedAccounts("shared1"));
        assertThrows(IllegalArgumentException.class, () -> SystemConfiguration.parseSharedAccounts("shared1:"));
    }

    @Test
    void parsesAddresses() {
        assertEquals(new InetSocketAddress("127.0.0.1", 9001), SystemConfiguration.parseAddress("127.0.0.1:9001"));
        assertThrows(IllegalArgumentException.class, () -> SystemConfiguration.parseAddress("127.0.0.1"));
    }

    @Test
    void loadsProvidedConfiguration() throws Exception {
        // Uses the cryptographic material and babel.conf distributed with the project
        Properties props = new Properties();
        try (var in = new java.io.FileInputStream("babel.conf")) {
            props.load(in);
        }
        SystemConfiguration config = SystemConfiguration.load(props);
        assertEquals(4, config.getN());
        assertEquals(1, config.getF());
        assertEquals("node1", config.getReplicaName(config.getReplicas().get(0)));
        assertEquals(new InetSocketAddress("127.0.0.1", 8003), config.getClientAddress("node3"));
        assertEquals(Set.of("client3", "client4", "client5"), config.getAccountOwners().get("shared2"));
        assertEquals(Set.of("client7"), config.getAccountOwners().get("client7"));
        assertEquals(config.getClientKeys().size() + 2, config.getAccountOwners().size());
    }
}

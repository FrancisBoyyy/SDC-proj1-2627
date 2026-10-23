package app.ledger;

import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import app.messages.Transfer;

/**
 * Keys and accounts used by the tests: clients client1..clientN own an account
 * each, and "shared" is owned by client1 and client2.
 */
final class TestAccounts {
    final Map<String, PrivateKey> privateKeys = new LinkedHashMap<>();
    final Map<String, PublicKey> publicKeys = new LinkedHashMap<>();
    final Map<String, Set<String>> owners = new LinkedHashMap<>();

    TestAccounts(int clients) throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        for (int i = 1; i <= clients; i++) {
            KeyPair pair = generator.generateKeyPair();
            privateKeys.put("client" + i, pair.getPrivate());
            publicKeys.put("client" + i, pair.getPublic());
            owners.put("client" + i, Set.of("client" + i));
        }
        owners.put("shared", Set.of("client1", "client2"));
    }

    Ledger newLedger(long initialBalance) {
        return new Ledger(owners, publicKeys, initialBalance);
    }

    Transfer transfer(String signer, String from, String to, long amount, long seq) {
        try {
            return Transfer.create(from, to, amount, seq, signer, privateKeys.get(signer));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    Transfer transfer(String from, String to, long amount, long seq) {
        return transfer(from, from, to, amount, seq);
    }
}

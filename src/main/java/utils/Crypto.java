package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class Crypto {

    public static final String CRYPTO_NAME_KEY = "crypto_name";
    public static final String KEY_STORE_LOCATION_KEY = "key_store_folder";
    public static final String KEY_STORE_PASSWORD_KEY = "key_store_password";
    public static final String TRUST_STORE_LOCATION_KEY = "trust_store";
    public static final String TRUST_STORE_PASSWORD_KEY = "trust_store_password";
    public static final String CLIENTS_KEY_STORE_LOCATION_KEY = "clients_key_store";
    public static final String CLIENTS_TRUST_STORE_LOCATION_KEY = "clients_trust_store";

    // All keys used in the project are Ed25519 keys (see scripts/generate-keys.sh)
    public static final String SIGNATURE_ALGORITHM = "Ed25519";
    public static final String DIGEST_ALGORITHM = "SHA-256";

    private Crypto() {
    }

    /**
     * Loads the key pair of this replica (named by the crypto_name property) from
     * its key store.
     */
    public static KeyPair getKeyPair(Properties props) throws KeyStoreException, IOException,
            NoSuchAlgorithmException, CertificateException, UnrecoverableEntryException {

        String alias = Objects.requireNonNull(props.getProperty(CRYPTO_NAME_KEY), CRYPTO_NAME_KEY + " must be defined");

        String keyStoreLocation = props.getProperty(KEY_STORE_LOCATION_KEY) + "/" + alias + ".ks";
        char[] keyStorePassword = props.getProperty(KEY_STORE_PASSWORD_KEY).toCharArray();

        KeyStore keyStore = loadKeyStore(keyStoreLocation, keyStorePassword);

        KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(keyStorePassword);
        KeyStore.PrivateKeyEntry keyEntry = (KeyStore.PrivateKeyEntry) keyStore.getEntry(alias, protParam);

        if (keyEntry == null) {
            throw new IllegalArgumentException("No private key entry found for alias: " + alias);
        }

        PrivateKey privateKey = keyEntry.getPrivateKey();
        Certificate cert = keyEntry.getCertificate();
        PublicKey publicKey = cert.getPublicKey();

        return new KeyPair(publicKey, privateKey);
    }

    public static KeyStore getTruststore(Properties props)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        return loadKeyStore(props.getProperty(TRUST_STORE_LOCATION_KEY),
                props.getProperty(TRUST_STORE_PASSWORD_KEY).toCharArray());
    }

    /**
     * Reads every certificate of a trust store, returning the public keys indexed
     * by alias and sorted by alias (so that every process builds the same order).
     */
    public static Map<String, PublicKey> getPublicKeys(KeyStore trustStore) throws KeyStoreException {
        List<String> aliases = new ArrayList<>(Collections.list(trustStore.aliases()));
        aliases.sort(AliasComparator.INSTANCE);
        Map<String, PublicKey> keys = new LinkedHashMap<>(aliases.size());
        for (String alias : aliases) {
            keys.put(alias, trustStore.getCertificate(alias).getPublicKey());
        }
        return keys;
    }

    /**
     * Public keys of all clients (the owners of the accounts of the system).
     */
    public static Map<String, PublicKey> getClientPublicKeys(Properties props)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore ks = loadKeyStore(props.getProperty(CLIENTS_TRUST_STORE_LOCATION_KEY),
                props.getProperty(TRUST_STORE_PASSWORD_KEY).toCharArray());
        return getPublicKeys(ks);
    }

    /**
     * Private keys of all clients. Only the client application should use this.
     */
    public static Map<String, PrivateKey> getClientPrivateKeys(Properties props) throws KeyStoreException,
            IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableEntryException {
        char[] password = props.getProperty(KEY_STORE_PASSWORD_KEY).toCharArray();
        KeyStore ks = loadKeyStore(props.getProperty(CLIENTS_KEY_STORE_LOCATION_KEY), password);
        List<String> aliases = new ArrayList<>(Collections.list(ks.aliases()));
        aliases.sort(AliasComparator.INSTANCE);
        Map<String, PrivateKey> keys = new LinkedHashMap<>(aliases.size());
        KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(password);
        for (String alias : aliases) {
            var entry = (KeyStore.PrivateKeyEntry) ks.getEntry(alias, protParam);
            keys.put(alias, entry.getPrivateKey());
        }
        return keys;
    }

    public static byte[] sign(byte[] data, PrivateKey key) throws GeneralSecurityException {
        Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
        signature.initSign(key);
        signature.update(data);
        return signature.sign();
    }

    /**
     * Verifies a signature. Never throws: malformed signatures or keys are simply
     * reported as invalid (they might have been produced by a Byzantine process).
     */
    public static boolean verify(byte[] data, byte[] signatureBytes, PublicKey key) {
        try {
            Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
            signature.initVerify(key);
            signature.update(data);
            return signature.verify(signatureBytes);
        } catch (GeneralSecurityException | RuntimeException e) {
            return false;
        }
    }

    public static byte[] digest(byte[] data) {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM).digest(data);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory in every Java implementation
            throw new IllegalStateException(e);
        }
    }

    public static String toHex(byte[] data) {
        return HexFormat.of().formatHex(data);
    }

    private static KeyStore loadKeyStore(String location, char[] password)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        try (FileInputStream fis = new FileInputStream(location)) {
            ks.load(fis, password);
        }
        return ks;
    }
}

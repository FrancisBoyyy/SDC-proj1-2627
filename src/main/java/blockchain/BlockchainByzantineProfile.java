package blockchain;

import java.util.Locale;

/**
 * Opt-in fault behaviors used by blockchain safety experiments.
 */
public enum BlockchainByzantineProfile {
    NONE("none"),
    WITHHOLD_BLOCKS("withhold_blocks"),
    BROADCAST_INVALID_BLOCK("broadcast_invalid_block"),
    SELECTIVE_SEND("selective_send");

    private final String propertyValue;

    BlockchainByzantineProfile(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    public static BlockchainByzantineProfile fromProperty(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);

        for (BlockchainByzantineProfile profile : values()) {
            if (profile.propertyValue.equals(normalized)) {
                return profile;
            }
        }

        throw new IllegalArgumentException(
                "Unknown blockchain Byzantine profile: " + value);
    }
}

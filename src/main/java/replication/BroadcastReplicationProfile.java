package replication;

import java.util.Locale;

/**
 * Opt-in faulty behaviors used in Byzantine reliable broadcast experiments.
 */
public enum BroadcastReplicationProfile {
    NONE("none"),
    SELECTIVE_SEND("selective_send");

    private final String propertyValue;

    BroadcastReplicationProfile(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    public static BroadcastReplicationProfile fromProperty(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (BroadcastReplicationProfile profile : values()) {
            if (profile.propertyValue.equals(normalized)) {
                return profile;
            }
        }
        throw new IllegalArgumentException(
                "Unknown BRB Byzantine profile: " + value);
    }
}

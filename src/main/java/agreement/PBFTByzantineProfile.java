package agreement;

import java.util.Locale;

/** Test-only Byzantine behavior profiles for PBFT experiments. */
public enum PBFTByzantineProfile {
    NONE,
    EQUIVOCATE_PRIMARY_ONCE;

    public static PBFTByzantineProfile fromProperty(String value) {
        String normalized = value == null ? "none" : value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "", "none" -> NONE;
            case "equivocate", "equivocate_primary", "equivocate_primary_once" ->
                    EQUIVOCATE_PRIMARY_ONCE;
            default -> throw new IllegalArgumentException(
                    "Unknown PBFT Byzantine profile: " + value);
        };
    }
}

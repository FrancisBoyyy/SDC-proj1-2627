package replication;

import java.util.Properties;

/**
 * Parameters of the SMR.
 */
public record SMRParameters(int batchSize, long batchTimeout, long suspectTimeout) {

    public SMRParameters {
        if (batchSize <= 0)
            throw new IllegalArgumentException("batch_size must be positive");
    }

    public static SMRParameters fromProperties(Properties props) {
        return new SMRParameters(
                Integer.parseInt(props.getProperty("batch_size", "10")),
                Long.parseLong(props.getProperty("batch_timeout", "2000")),
                Long.parseLong(props.getProperty("suspect_timeout", "2000"))
        );
    }
}

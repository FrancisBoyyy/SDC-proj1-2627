package agreement;

import java.util.Properties;

/**
 * Parameters of the SMR.
 */
public record PBFTParameters(int checkpointInterval, int logSize, int retain, int fetchRangeCap, long fetchGraceTime, long fetchRetryTime) {

    public static PBFTParameters fromProperties(Properties props) {
        return new PBFTParameters(
                Integer.parseInt(props.getProperty("checkpoint_interval", "100")),
                Integer.parseInt(props.getProperty("log_size", "200")),
                Integer.parseInt(props.getProperty("retain", "1000")),
                Integer.parseInt(props.getProperty("fetch_range_cap", "1000")),
                Long.parseLong(props.getProperty("fetch_grace_time", "300")),
                Long.parseLong(props.getProperty("fetch_retry_time", "300"))
        );
    }
}

package client;

import java.util.Properties;

/**
 * Parameters of the automated client (see the client section of babel.conf).
 */
public record ClientParameters(int firstClient, int numClients, int transferPercentage, int historyPercentage,
        int sharedPercentage, int maxAmount, long operationTimeout, long prepareTime, long runTime,
        long cooldownTime, String resultsFile, String label) {

    public ClientParameters {
        if (numClients <= 0 || firstClient <= 0) {
            throw new IllegalArgumentException("first_client and num_clients must be positive");
        }
        if (transferPercentage < 0 || historyPercentage < 0 || transferPercentage + historyPercentage > 100) {
            throw new IllegalArgumentException("Invalid operation mix (percentages)");
        }
        if (maxAmount <= 0) {
            throw new IllegalArgumentException("max_amount must be positive");
        }
    }

    public static ClientParameters fromProperties(Properties props) {
        return new ClientParameters(
                Integer.parseInt(props.getProperty("first_client", "1")),
                Integer.parseInt(props.getProperty("num_clients", "10")),
                Integer.parseInt(props.getProperty("transfer_percentage", "80")),
                Integer.parseInt(props.getProperty("history_percentage", "0")),
                Integer.parseInt(props.getProperty("shared_percentage", "0")),
                Integer.parseInt(props.getProperty("max_amount", "10")),
                Long.parseLong(props.getProperty("operation_timeout", "5000")),
                Long.parseLong(props.getProperty("prepare_time", "3000")),
                Long.parseLong(props.getProperty("run_time", "30000")),
                Long.parseLong(props.getProperty("cooldown_time", "5000")),
                props.getProperty("results_file", "results.csv"),
                props.getProperty("label", "experiment"));
    }
}

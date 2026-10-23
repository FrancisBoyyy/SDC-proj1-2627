package client.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Collects the latency of the operations executed by the clients, and computes
 * the throughput (operations completed per second) and latency statistics of
 * each type of operation.
 *
 * <p>
 * Only operations that complete inside the measurement window (between
 * {@link #start()} and {@link #stop()}) are considered.
 */
public final class Measurements {

    private static final class OperationStats {
        private long[] latencies = new long[1024];
        private int count;
        private long failed;
        private long timeouts;
        private final TreeMap<Long, Long> perSecond = new TreeMap<>();

        private void add(long latencyNanos, long second) {
            if (count == latencies.length) {
                latencies = Arrays.copyOf(latencies, count * 2);
            }
            latencies[count++] = latencyNanos;
            perSecond.merge(second, 1L, Long::sum);
        }
    }

    public record Summary(String operation, long completed, long failed, long timeouts, double throughput,
            double avgLatencyMs, double p50LatencyMs, double p95LatencyMs, double p99LatencyMs) {
    }

    private final Map<String, OperationStats> stats;
    private long startNanos;
    private long stopNanos;
    private boolean measuring;

    public Measurements() {
        this.stats = new LinkedHashMap<>();
    }

    public void start() {
        startNanos = System.nanoTime();
        measuring = true;
    }

    public void stop() {
        stopNanos = System.nanoTime();
        measuring = false;
    }

    public boolean isMeasuring() {
        return measuring;
    }

    /**
     * An operation completed successfully.
     */
    public void success(String operation, long startedAtNanos, long endedAtNanos) {
        if (measuring && startedAtNanos >= startNanos) {
            stats(operation).add(endedAtNanos - startedAtNanos, (endedAtNanos - startNanos) / 1_000_000_000L);
        }
    }

    /**
     * An operation completed, but was not successful (e.g., a conflicting
     * transfer).
     */
    public void failure(String operation) {
        if (measuring) {
            stats(operation).failed++;
        }
    }

    /**
     * The client gave up waiting for the replies to an operation.
     */
    public void timeout(String operation) {
        if (measuring) {
            stats(operation).timeouts++;
        }
    }

    public Map<String, Summary> summaries() {
        double seconds = Math.max(1e-9, ((measuring ? System.nanoTime() : stopNanos) - startNanos) / 1e9);
        Map<String, Summary> result = new LinkedHashMap<>();
        for (var entry : stats.entrySet()) {
            OperationStats s = entry.getValue();
            long[] sorted = Arrays.copyOf(s.latencies, s.count);
            Arrays.sort(sorted);
            double avg = 0;
            for (long l : sorted) {
                avg += l;
            }
            avg = sorted.length == 0 ? 0 : avg / sorted.length;
            result.put(entry.getKey(), new Summary(entry.getKey(), s.count, s.failed, s.timeouts,
                    s.count / seconds, avg / 1e6, percentile(sorted, 0.50) / 1e6,
                    percentile(sorted, 0.95) / 1e6, percentile(sorted, 0.99) / 1e6));
        }
        return result;
    }

    /**
     * Writes a CSV file with one line per operation type (summary), followed by
     * the number of operations completed in each second of the experiment.
     */
    public void writeCsv(Path file, String experimentLabel) throws IOException {
        try (var out = new PrintWriter(Files.newBufferedWriter(file))) {
            out.println("label,operation,completed,failed,timeouts,throughput_ops,avg_ms,p50_ms,p95_ms,p99_ms");
            for (Summary s : summaries().values()) {
                out.printf("%s,%s,%d,%d,%d,%.2f,%.3f,%.3f,%.3f,%.3f%n", experimentLabel, s.operation(),
                        s.completed(), s.failed(), s.timeouts(), s.throughput(), s.avgLatencyMs(),
                        s.p50LatencyMs(), s.p95LatencyMs(), s.p99LatencyMs());
            }
            out.println();
            out.println("label,operation,second,completed");
            for (var entry : stats.entrySet()) {
                for (var second : entry.getValue().perSecond.entrySet()) {
                    out.printf("%s,%s,%d,%d%n", experimentLabel, entry.getKey(), second.getKey(),
                            second.getValue());
                }
            }
        }
    }

    private OperationStats stats(String operation) {
        return stats.computeIfAbsent(operation, k -> new OperationStats());
    }

    private static double percentile(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }
}

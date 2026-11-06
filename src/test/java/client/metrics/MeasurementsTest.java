package client.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class MeasurementsTest {

    private static final long MS = 1_000_000L;

    @Test
    void statisticsOfCompletedOperations() {
        Measurements m = new Measurements();
        m.start();
        long now = System.nanoTime();
        // Latencies of 1, 2, ..., 100 ms
        for (int i = 1; i <= 100; i++) {
            m.success("transfer", now, now + i * MS);
        }
        m.failure("transfer");
        m.timeout("transfer");
        m.timeout("transfer");
        m.stop();

        var s = m.summaries().get("transfer");
        assertEquals(100, s.completed());
        assertEquals(1, s.failed());
        assertEquals(2, s.timeouts());
        assertEquals(50.5, s.avgLatencyMs(), 1e-6);
        assertEquals(50.0, s.p50LatencyMs(), 1e-6);
        assertEquals(95.0, s.p95LatencyMs(), 1e-6);
        assertEquals(99.0, s.p99LatencyMs(), 1e-6);
        assertTrue(s.throughput() > 0);
    }

    @Test
    void onlyOperationsInsideTheMeasurementWindowCount() {
        Measurements m = new Measurements();
        long before = System.nanoTime();
        m.success("balance", before, before + MS); // before start: ignored
        m.start();
        m.success("balance", before, System.nanoTime()); // started before the window: ignored
        long t = System.nanoTime();
        m.success("balance", t, t + MS);
        m.stop();
        m.success("balance", t, t + MS); // after stop: ignored
        m.timeout("balance"); // after stop: ignored
        var s = m.summaries().get("balance");
        assertEquals(1, s.completed());
        assertEquals(0, s.timeouts());
    }

    @Test
    void writesCsv() throws Exception {
        Measurements m = new Measurements();
        m.start();
        long t = System.nanoTime();
        m.success("transfer", t, t + 3 * MS);
        m.stop();
        Path file = Files.createTempFile("results", ".csv");
        m.writeCsv(file, "test");
        var lines = Files.readAllLines(file);
        assertTrue(lines.get(0).startsWith("label,operation,completed"));
        assertTrue(lines.get(1).startsWith("test,transfer,1,0,0,"));
        assertTrue(lines.stream().anyMatch(l -> l.startsWith("label,operation,second,completed")));
        Files.delete(file);
    }
}

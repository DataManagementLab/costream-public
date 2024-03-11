package costream.plan.executor.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class Stats {
    private final long start;
    private long windowStart;
    private final ArrayList<Integer> latencies;
    private final int sampling;
    private int iteration;
    private int index;
    private long count;
    private long bytes;
    private int maxLatency;
    private long totalLatency;
    private long windowCount;
    private int windowMaxLatency;
    private long windowTotalLatency;
    private long windowBytes;
    private final long reportingInterval;

    private final Logger logger = LoggerFactory.getLogger("kafka");

    public Stats(int reportingInterval) {
        this.start = System.currentTimeMillis();
        this.windowStart = System.currentTimeMillis();
        this.iteration = 0;
        //this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
        this.sampling = 1;
        this.latencies = new ArrayList<>(); // new int[(int) (numRecords / this.sampling) + 1];
        this.index = 0;
        this.maxLatency = 0;
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
        this.totalLatency = 0;
        this.reportingInterval = reportingInterval;
    }

    public void record(int iter, int latency, int bytes, long time) {
        this.count++;
        this.bytes += bytes;
        this.totalLatency += latency;
        this.maxLatency = Math.max(this.maxLatency, latency);
        this.windowCount++;
        this.windowBytes += bytes;
        this.windowTotalLatency += latency;
        this.windowMaxLatency = Math.max(windowMaxLatency, latency);
        if (iter % this.sampling == 0) {
            this.latencies.add(latency);
            this.index++;
        }
        /* maybe report the recent perf */
        if (time - windowStart >= reportingInterval) {
            //printWindow();
            newWindow();
        }
    }

    public Callback nextCompletion(long start, int bytes, Stats stats) {
        Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
        this.iteration++;
        return cb;
    }

    public void printWindow() {
        long elapsed = System.currentTimeMillis() - windowStart;
        double recsPerSec = round(1000.0 * windowCount / (double) elapsed, 2);
        double mbPerSec = round(1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0), 2);
        double averageLatency = round(windowTotalLatency / (double) windowCount, 2);
        logger.info("{} records/sec sent, {} records send, ({} MB/sec), {}ms avg latency, {} ms max latency",
                recsPerSec, windowCount, mbPerSec, averageLatency, (double) windowMaxLatency);
    }

    public void newWindow() {
        this.windowStart = System.currentTimeMillis();
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public void printTotal() {
        long elapsed = System.currentTimeMillis() - start;
        double recsPerSec = round(1000.0 * count / (double) elapsed, 2);
        double mbPerSec = round((1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0)), 2);
        int[] percentiles = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
        double averageLatency = round(totalLatency / (double) count, 2);
        logger.info("TupleProducer ended. {} records sent, {} records/sec ({} MB/sec), {} ms avg latency, " +
                        "{} ms max latency, {} ms 50th, {} ms 95th, {} ms 99th, {} ms 99.9th",
                count,
                recsPerSec,
                mbPerSec,
                averageLatency,
                maxLatency,
                percentiles[0],
                percentiles[1],
                percentiles[2],
                percentiles[3]);
    }

    private static int[] percentiles(ArrayList<Integer> latencies, int count, double... percentiles) {
        int size = Math.min(count, latencies.size());
        ArrayList<Integer> sortedLatencies = latencies
                .stream()
                .sorted()
                .collect(Collectors.toCollection(ArrayList::new));
        int[] values = new int[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            int index = (int) (percentiles[i] * size);
            values[i] = sortedLatencies.get(index);
        }
        return values;
    }
}
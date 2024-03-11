package costream.plan.executor.kafka;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public final class PerfCallback implements Callback {
    private final long start;
    private final int iteration;
    private final int bytes;
    private final Stats stats;

    public PerfCallback(int iter, long start, int bytes, Stats stats) {
        this.start = start;
        this.stats = stats;
        this.iteration = iter;
        this.bytes = bytes;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long now = System.currentTimeMillis();
        int latency = (int) (now - start);
        this.stats.record(iteration, latency, bytes, now);
        if (exception != null)
            exception.printStackTrace();
    }
}

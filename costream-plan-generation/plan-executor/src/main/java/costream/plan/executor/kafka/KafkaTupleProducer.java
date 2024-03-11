package costream.plan.executor.kafka;

import costream.plan.executor.utils.Triple;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class KafkaTupleProducer implements Runnable {
    private final KafkaProducer<byte[], byte[]> producer;
    protected final Triple<Integer, Integer, Integer> tupleWidth;
    final Thread thread;
    private final String topic;
    protected final String queryName;
    private final AtomicBoolean terminated;
    private final float throughput;


    public KafkaTupleProducer(String queryName, String topic, Properties props, float throughputPerThread, Triple<Integer, Integer, Integer> tupleWidth) {
        this.queryName = queryName;
        this.topic = topic;
        this.producer = new KafkaProducer<>(props);
        this.thread = new Thread(this, topic);
        this.terminated = new AtomicBoolean(false);
        this.tupleWidth = tupleWidth;
        this.throughput = throughputPerThread;
    }
    @Override
    public void run() {
        ProducerRecord<byte[], byte[]> record;

        Stats stats = new Stats(5000);
        ThroughputThrottler throttler = new ThroughputThrottler((long) this.throughput, System.currentTimeMillis());

        int i = 0;
        while (!terminated.get()) {
            i += 1;
            String nextTuple = nextTuple();
            record = new ProducerRecord<>(this.topic, nextTuple.getBytes(StandardCharsets.UTF_8));
            long sendStartMs = System.currentTimeMillis();
            Callback cb = stats.nextCompletion(sendStartMs, nextTuple.toString().getBytes(StandardCharsets.UTF_8).length, stats);
            producer.send(record, cb);
            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }
        // Make sure all messages are sent before printing out the stats and the metrics
        producer.flush();
        stats.printTotal();
        producer.close();
    }

    public void terminate() {
        terminated.set(true);
    }

    public abstract String nextTuple();
}
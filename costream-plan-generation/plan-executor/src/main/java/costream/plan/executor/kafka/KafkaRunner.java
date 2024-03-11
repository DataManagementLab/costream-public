package costream.plan.executor.kafka;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import costream.plan.executor.main.Constants;
import costream.plan.executor.utils.EnrichedQueryGraph;
import costream.plan.executor.utils.GraphUtils;
import costream.plan.executor.utils.MongoClientProvider;
import costream.plan.executor.utils.Triple;
import costream.plan.executor.application.smartgrid.SmartGridTupleProducer;
import costream.plan.executor.application.synthetic.SyntheticTupleProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.storm.Config;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaRunner {
    private final Logger logger = LoggerFactory.getLogger("kafka");
    private final ArrayList<KafkaTupleProducer> kafkaTupleProducers;
    private String queryName;
    private final Properties consumerProperties;
    private final Properties producerProperties;
    private final boolean localExecution;
    private MongoCollection<Document> collection;
    private int numSources;
    private long startTime;
    private final int numOfThreads;
    private String mode;

    public KafkaRunner(Config config, String mode) {
        this(config, Constants.Kafka.NUM_THREADS, mode);
    }

    public KafkaRunner(Config config, int numOfThreads, String mode) {
        this.localExecution = config.get("storm.cluster.mode").equals("local");
        this.numOfThreads = numOfThreads;
        this.mode = mode;
        if (!this.localExecution) {
            MongoClient mongoClient = MongoClientProvider.getMongoClient(config);
            this.collection = mongoClient
                    .getDatabase((String) config.get("mongo.database"))
                    .getCollection((String) config.get("mongo.collection.offsets"));
        }
        kafkaTupleProducers = new ArrayList<>();

        producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap.server") + ":" + config.get("kafka.port"));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "0");

        consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "dsps");
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap.server") + ":" + config.get("kafka.port"));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    public void start(String queryName, ArrayList<Integer> eventRates, ArrayList<Triple<Integer, Integer, Integer>> tupleWidths) {
        this.queryName = queryName;
        logger.info("Starting {} KafkaTupleProducer of mode {} for executing query {}", this.numOfThreads, this.mode, this.queryName);

        numSources = eventRates.size();
        assert numSources == tupleWidths.size();
        logger.info("Event rates: {} and tuple widths {} were identified in query {}",
                eventRates,
                tupleWidths,
                this.queryName);

        // initialize threads and start them
        kafkaTupleProducers.clear();
        for (int topicIdx = 0; topicIdx < numSources; topicIdx++) {

            logger.info("Starting {} KafkaTupleProducers for query {} on topic {}",
                    this.numOfThreads, this.queryName,
                    Constants.Kafka.TOPICS[topicIdx]);

            float throughputPerThread = eventRates.get(topicIdx) / (float) this.numOfThreads; //events per second
            logger.info("KafkaTupleProducers for query {} have throughput of {} ev/s and interval: " +
                            "{}ns to reach desired thread event rate of {}",
                    this.queryName,
                    throughputPerThread,
                    getInterval(throughputPerThread),
                    eventRates.get(topicIdx));

            for (int threadIdx = 0; threadIdx < this.numOfThreads; threadIdx++) {
                // assigning unique Client_ID is afforded
                producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
                        Constants.Kafka.TOPICS[topicIdx] + "-" + threadIdx + "-" + this.queryName);

                KafkaTupleProducer producer;
                if (this.mode.startsWith("ad")) {
                    producer = new KafkaFileReadTupleProducer(
                            this.queryName,
                            Constants.Kafka.TOPICS[topicIdx],
                            (Properties) producerProperties.clone(),
                            throughputPerThread,
                            tupleWidths.get(topicIdx),
                            mode,
                            "./data/ad_clicks.dat"
                    );
                } else if (this.mode.equals(Constants.QueryType.BENCHMARK.SPIKE_DETECTION)) {
                    producer = new KafkaFileReadTupleProducer(
                            this.queryName,
                            Constants.Kafka.TOPICS[topicIdx],
                            (Properties) producerProperties.clone(),
                            throughputPerThread,
                            tupleWidths.get(topicIdx),
                            mode,
                            "./data/sensors.dat"
                    );
                } else if (this.mode.startsWith(Constants.QueryType.BENCHMARK.SMART_GRID)) {
                    producer = new SmartGridTupleProducer(
                            this.queryName,
                            Constants.Kafka.TOPICS[topicIdx],
                            (Properties) producerProperties.clone(),
                            throughputPerThread,
                            tupleWidths.get(topicIdx)
                    );
                } else {
                    producer = new SyntheticTupleProducer(
                            this.queryName,
                            Constants.Kafka.TOPICS[topicIdx],
                            (Properties) producerProperties.clone(),
                            throughputPerThread,
                            tupleWidths.get(topicIdx));
                }
                kafkaTupleProducers.add(producer);
                producer.thread.start();
            }
        }
        startTime = System.currentTimeMillis();
        logger.info("All KafkaTupleProducer for query {} are started", this.queryName);
    }

    /**
     * Starting a set of KafkaProducers that are executed in parallel, the amount is specified in Constants.
     *
     * @param query Query to start KafkaProducers for.
     */
    public void start(EnrichedQueryGraph query) {
        // Get event rates and tuple widths out of the query graph
        ArrayList<Integer> eventRates = GraphUtils.getEventRates(query.getGraph());
        ArrayList<Triple<Integer, Integer, Integer>> tupleWidths = GraphUtils.getTupleWidths(query.getGraph());
        this.start(query.getName(), eventRates, tupleWidths);
    }

    private long getInterval(float throughputPerThread) {
        long nanos;
        long millis;
        // Compute interval in millis and nanos
        double mil = 1.0 / throughputPerThread * 1000;
        if (mil < 1) {
            nanos = Math.round(1.0 / throughputPerThread * 1000000000);
            millis = 0;
        } else {
            nanos = Math.round((mil - Math.floor(mil)) * 1000000);
            millis = (long) Math.floor(mil);
        }
        return nanos + TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /**
     * Stop the KafkaProducers. Identify the last offset of the producers, e.g. the Kafka offset that is the last one.
     * Write this to local file or database, as we compute the queuing offset with that information. To gather the offset,
     * a new temporary consumer needs to be created.
     */
    public void stop() {
        logger.info("Stop KafkaTupleProducer for query {}", this.queryName);
        long endTime = System.currentTimeMillis();

        for (KafkaTupleProducer prod : kafkaTupleProducers) {
            prod.terminate();
        }
        kafkaTupleProducers.clear();

        // identify last offset of the query
        logger.info("Writing producer offsets of query {} to the database", this.queryName);
        for (int topicIdx = 0; topicIdx < numSources; topicIdx++) {
            consumerProperties.put("client.id", this.queryName + "-" + Constants.Kafka.TOPICS[topicIdx] + "-end-offset-client");
            KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerProperties);
            TopicPartition partition = new TopicPartition(Constants.Kafka.TOPICS[topicIdx], 0);
            consumer.assign(Collections.singleton(partition));
            consumer.seekToEnd(Collections.singleton(partition));
            long position = consumer.position(partition);
            consumer.close();

            // write offset to disk or database
            HashMap<String, Object> offset = new HashMap<>();
            offset.put(Constants.QueryProperties.QUERY, this.queryName);
            offset.put(Constants.OperatorProperties.PRODUCER_POSITION, position);
            offset.put(Constants.OperatorProperties.TOPIC, Constants.Kafka.TOPICS[topicIdx]);
            offset.put(Constants.OperatorProperties.KAFKA_DURATION, (endTime - startTime));
            Document document = new Document();
            document.putAll(offset);
            if (localExecution) {
                Logger newLogger = LoggerFactory.getLogger("offsets");
                newLogger.info(document.toJson());
            } else {
                collection.insertOne(document);
            }
        }
    }
}
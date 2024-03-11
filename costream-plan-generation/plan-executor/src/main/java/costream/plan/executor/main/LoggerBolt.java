package costream.plan.executor.main;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import costream.plan.executor.utils.MongoClientProvider;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LoggerBolt implements IRichBolt {
    private boolean initialized;
    private long startTime;
    private long lastEntryTime;
    private final ArrayList<Long> arrivalTimestamps;
    private int inputCounter;
    private final ArrayList<Long> endToEndLatencies;
    private final ArrayList<Long> processingLatencies;
    private String queryName;
    private boolean localExecution;
    private MongoCollection<Document> collection;
    private MongoClient mongoClient;
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private int taskId;

    /**
     * This is a standard storm bolt that is executed separately on an own node. It logs the output labels
     * (currently event rate and end-to-end-latency). It uses the "label"-logger from the adaptive log4j-config file.
     * The labels are logged on each worker locally in a common log-file having the format:
     * {name="query123", throughput= 9876, latency = 0.233}
     */
    public LoggerBolt() {
        this.initialized = false;
        this.startTime = 0;
        this.lastEntryTime = 0;
        this.inputCounter = 0;
        this.endToEndLatencies = new ArrayList<>();
        this.processingLatencies = new ArrayList<>();
        this.arrivalTimestamps = new ArrayList<>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map<String, Object> config, TopologyContext context, OutputCollector collector) {
        this.localExecution = config.get("storm.cluster.mode").equals("local");
        this.queryName = (String) config.get("topology.name");

        this.taskId = context.getThisTaskId();
        logger.info("LoggerBolt created for query {} as component {} and task {}", this.queryName,
                context.getThisComponentId(),
                this.taskId);

        if (!this.localExecution) {
            this.mongoClient = MongoClientProvider.getMongoClient(config);
            this.collection = mongoClient
                    .getDatabase((String) config.get("mongo.database"))
                    .getCollection((String) config.get("mongo.collection.labels"));

            mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
            collection = collection.withWriteConcern(WriteConcern.ACKNOWLEDGED);
        }
    }

    /**
     * Initialize a start time after first tuple arrives to later calculate the event rate
     *
     * @param input The input tuple to be processed.
     */
    @Override
    public void execute(Tuple input) {
        this.lastEntryTime = System.currentTimeMillis();

        if (!initialized) {
            initialized = true;
            this.startTime = this.lastEntryTime;
        }
        this.inputCounter++;
        this.arrivalTimestamps.add(this.lastEntryTime);

        Long oldestTimestamp;
        Long youngestTimestamp;
        String tupleQueryName;
        try {
            DataTuple val = (DataTuple) input.getValue(0);
            oldestTimestamp = val.getE2ETimestamp();
            youngestTimestamp = val.getProcessingTimestamp();
            tupleQueryName = val.getQueryName();
        } catch (ClassCastException e) {
            try {
                Pair<Object, DataTuple> val = (Pair<Object, DataTuple>) input.getValue(0);
                oldestTimestamp = val.getSecond().getE2ETimestamp();
                youngestTimestamp = val.getSecond().getProcessingTimestamp();
                tupleQueryName = val.getSecond().getQueryName();
            } catch (ClassCastException e1) {
                oldestTimestamp = ((DataTuple) input.getValue(1)).getE2ETimestamp();
                youngestTimestamp = ((DataTuple) input.getValue(1)).getProcessingTimestamp();
                tupleQueryName = ((DataTuple) input.getValue(1)).getQueryName();
            }
        }
        if (!tupleQueryName.equals(queryName)) {
            throw new RuntimeException("Tuple queryName: " + tupleQueryName + " not equals to queryName " + queryName);
        }
        endToEndLatencies.add(this.lastEntryTime - oldestTimestamp);
        processingLatencies.add(this.lastEntryTime - youngestTimestamp);
    }

    /**
     * The logging happens at cleanup /destruction
     */
    @Override
    public void cleanup() {
        long elapsedTime = this.lastEntryTime - this.startTime;
        double seconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);

        logger.info("Cleanup called of LoggerBolt-{} for query {}", queryName, taskId);
        logger.info("{} number of tuples arrived at LoggerBolt-{} during execution length of {}",
                this.inputCounter, this.taskId, seconds);

        HashMap<String, Object> label = new HashMap<>();
        if (this.inputCounter == 0) {
            label.put(Constants.QueryLabels.THROUGHPUT, "null");
            label.put(Constants.QueryLabels.E2ELATENCY, "null");
            label.put(Constants.QueryLabels.PROCLATENCY, "null");
            label.put(Constants.QueryLabels.COUNTER, "null");
            label.put(Constants.QueryProperties.DURATION, "null");

        } else {
           long startCompute = System.currentTimeMillis();

            // Throughput
            double tpt = (double) this.inputCounter / ((this.lastEntryTime - this.startTime) / 1000.0);
            label.put(Constants.QueryLabels.TPT_MEAN, tpt);

            // E2E-Latency
            double e2eMean = this.endToEndLatencies.stream().mapToDouble(val -> val).average().orElse(0.0);
            label.put(Constants.QueryLabels.E2E_MEAN, e2eMean);

            // Proc-Latency
            double procMean = this.processingLatencies.stream().mapToDouble(val -> val).average().orElse(0.0);
            label.put(Constants.QueryLabels.PROC_MEAN, procMean);

            label.put(Constants.QueryLabels.COUNTER, this.inputCounter);
            label.put(Constants.QueryProperties.DURATION, seconds);

            logger.info("LoggerBolt-{} required {} ms to compute label", taskId, (System.currentTimeMillis() - startCompute));
        }

        label.put(Constants.QueryProperties.QUERY, this.queryName);
        Document document = new Document();
        document.putAll(label);

        logger.info("LoggerBolt-{}: Logging label {} for query {}", taskId, document.toJson(), this.queryName);

        if (localExecution) {
            logger = LoggerFactory.getLogger("labels");
            logger.info(document.toJson());
        } else {
            collection.insertOne(document);
            mongoClient.close();
        }
    }
}

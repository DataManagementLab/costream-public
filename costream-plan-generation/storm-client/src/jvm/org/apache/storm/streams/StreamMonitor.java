package org.apache.storm.streams;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.streams.processors.AggregateByKeyProcessor;
import org.apache.storm.streams.processors.AggregateProcessor;
import org.apache.storm.streams.processors.BaseProcessor;
import org.apache.storm.streams.processors.FilterProcessor;
import org.apache.storm.streams.processors.JoinProcessor;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is entirely new and added to the storm sources.
 * The StreamMonitor is attached to the processor nodes that are used in the Stream API.
 * It is called for every incoming and outgoing event and keeps track of the data characteristics
 * (like tuple width, selectivities, etc.)
 * As no shutdown hooks can be reveiced here when shutting down the topology, the measurement has a defined length
 * It starts when the first tuple arrives. At the end, the data characteristics are written into the logs per operator.
 */
public class StreamMonitor implements Serializable {
    private final HashMap<String, Object> description;
    private boolean initialized;
    private boolean observationMade;
    private long startTime;
    private int inputCounter;
    private int outputCounter;
    private boolean localMode;
    private final BaseProcessor<?> processor;
    private final Map<String, Object> topoConf;
    private final ArrayList<Double> joinSelectivities;
    private final ArrayList<Integer> windowLengths;
    private final Integer[] joinTupleWidths;
    private Logger localObservationLogger;
    private final Logger logger;
    private MongoClient mongoClient;

    private DecimalFormat decimalFormat;

    public StreamMonitor(HashMap<String, Object> description, BaseProcessor<?> processor) {
        this.topoConf = Utils.readStormConfig();
        this.description = description;
        this.description.put("tupleWidthIn", "-1");
        this.description.put("tupleWidthOut", "-1");
        this.description.put("realSelectivity", "-1");
        this.initialized = false;
        this.observationMade = false;
        this.processor = processor;
        this.joinSelectivities = new ArrayList<>();
        this.windowLengths = new ArrayList<>();
        this.joinTupleWidths = new Integer[] {null, null};
        this.logger = LoggerFactory.getLogger(this.getClass());
        DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        df.setMaximumFractionDigits(6);
        this.decimalFormat = df;
    }

    public <T> void reportInput(T input) {
        if (!initialized) {
            initialized = true;
            logger.info("StreamMonitor initialized for {}", this.description.get("id"));
            // bug: The topoConf somehow has not the correct mode of "storm.cluster.mode", so checking is done
            // via nimbus.seeds
            if (((ArrayList<String>) topoConf.get("nimbus.seeds")).get(0).equals("localhost")) {
                this.localMode = true;
                this.localObservationLogger = LoggerFactory.getLogger("observation");
            } else {
                this.localMode = false;
                MongoCredential credential = MongoCredential.createCredential("dsps_mongo", "dsps", "s3cure!".toCharArray());
                mongoClient = new MongoClient(
                        new ServerAddress(((ArrayList<String>) topoConf.get("nimbus.seeds")).get(0), 27017),
                        Collections.singletonList(credential));
            }
            this.startTime = System.currentTimeMillis();
            description.put("tupleWidthIn", getTupleSize(input));
            description.put("tupleWidthOut", -1); // not any observation yet
        }
        this.inputCounter++;
    }

    public <T> void reportOutput(T output) {
        if (this.processor instanceof AggregateByKeyProcessor) {
            // in this case it is the number of identified join keys
            description.put("tupleWidthOut", ((HashMap<?, ?>) output).size());
        } else {
            description.put("tupleWidthOut", getTupleSize(output));
        }
        this.outputCounter++;
    }

    public void reportJoinSelectivity(int size1, int size2, int joinPartners) {
        if (size1 != 0 && size2 != 0) {
            this.joinSelectivities.add((double) joinPartners / (double) (size1 * size2));
        }
    }

    public <T> void reportJoinTupleWidth(String side, T value) {
        if (side.equals("left")) {
            this.joinTupleWidths[0] = getTupleSize(value);
        } else if (side.equals("right")) {
            this.joinTupleWidths[1] = getTupleSize(value);
        }
    }

    public <T> void reportWindowLength(T state) {
        int length;
        if (state instanceof Long || state instanceof Double || state instanceof Integer) {
            length = 1;
        } else {
            try {
                length = ((ArrayList<Values>) state).size();
            } catch (ClassCastException e1) {
                Pair<Object, Object> p = (Pair<Object, Object>) state;
                length = ((ArrayList<Values>) p.getSecond()).size();
            }
        }
        this.windowLengths.add(length);
    }

    public String round(double value) {
        return this.decimalFormat.format(value);
    }

    private String computeSelectivity() {
        // Compute selectivities if applicable
        double selectivity = -1.0;
        if (this.processor instanceof JoinProcessor) {
            selectivity = this.joinSelectivities.stream().mapToDouble(val -> val).average().orElse(0.0);
        } else if (this.processor instanceof FilterProcessor || this.processor instanceof AggregateByKeyProcessor) {
            selectivity = (double) this.outputCounter / (double) this.inputCounter;
        } else if (this.processor instanceof AggregateProcessor) {
            selectivity = 1.0 / (this.windowLengths.stream().mapToDouble(val -> val).average().orElse(0.0));
        } else {
            logger.error("Selectivites for " + this.processor + "not implemented");
        }
        return round(selectivity);
    }

    protected void cleanup() {
        long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.startTime);
        logger.info("StreamMonitor end triggered for {}", this.processor.getDescription().get("id"));

        description.put("outputRate", round((double) this.outputCounter  / (double) elapsedSeconds));
        description.put("inputRate", round((double) this.inputCounter / (double) elapsedSeconds));
        description.put("inputCounter", this.inputCounter);
        description.put("outputCounter", this.outputCounter);

        if (this.outputCounter == 0) {
            description.put("realSelectivity", "0");
        } else {
            description.put("realSelectivity", computeSelectivity());
        }
        if (this.inputCounter == 0) {
            description.put("realSelectivity", "-1");
        }

        // Overwrite join input tuple width with the sum of both single tuple widths
        if (this.processor instanceof JoinProcessor) {
            Integer sumOfTupleWidths = Arrays.stream(this.joinTupleWidths).mapToInt(val -> val).sum();
            description.put("tupleWidthIn", sumOfTupleWidths);
        }

        Document json = new Document();
        json.putAll(description);

        logger.info("StreamMonitor reporting observation for: {}: {} ", this.description.get("id"), this.description);

        // Log data characteristics either locally ore in database
        if (this.description.get("id") != null && this.localMode) {
            localObservationLogger.info(json.toJson());
        } else if (this.description.get("id") != null && !this.localMode) {
            MongoCollection<Document> collection = mongoClient.getDatabase("dsps").getCollection("query_observations");
            collection.insertOne(json);
            mongoClient.close();
        }
    }

    private <T> int getTupleSize(T input) {
        if (input instanceof Integer || input instanceof String || input instanceof Double || input instanceof Long) {
            return 1;
        }

        int size = 0;
        try {
            Values v = (Values) input;
            size = v.size();
        } catch (ClassCastException e1) {
            try {
                TupleImpl v = (TupleImpl) input;
                size = v.size();
            } catch (ClassCastException e2) {
                Pair<Object, Object> p = (Pair<Object, Object>) input;
                try {
                    Values v = (Values) p.getSecond();
                    if (p.getSecond() != null) {
                        size = v.size();
                    }
                } catch (ClassCastException e3) {
                    if (p.getSecond() instanceof Integer || p.getSecond() instanceof String || p.getSecond()
                            instanceof Double || p.getSecond() instanceof Long) {
                        size = 1;
                    } else {
                        ArrayList<Values> l = (ArrayList<Values>) p.getSecond();
                        size = l.get(0).size();
                    }
                }
            }
        }
        return size;
    }
}

package costream.plan.executor.utils;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import costream.plan.executor.main.Constants;
import costream.plan.executor.main.MongoReadException;
import org.apache.storm.Config;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MongoReader {

    public static void addComponentsFromDbToGraph(Graph graph, MongoCollection<Document> groupingCollection) {
        Logger log = LoggerFactory.getLogger("scheduler");
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            String id = (String) node.getAttribute("id");
            if (GraphUtils.isOperator(node)) {
                Document grouping = groupingCollection.find(eq("operator", id)).first();
                if (grouping == null) {
                    throw new RuntimeException("Couldn't find component " + id + " in Mongo");
                }
                if (grouping.containsKey(Constants.OperatorProperties.COMPONENT)) {
                    String component = (String) grouping.get(Constants.OperatorProperties.COMPONENT);
                    node.setAttribute(Constants.OperatorProperties.COMPONENT, component);
                    log.info("Found component: {} for node {}", component, node.getId());
                }
            }
        }
    }

    public static void readOffsetsToGraph(Graph graph, MongoCollection<Document> offsetCollection) throws InterruptedException {
        Logger log = LoggerFactory.getLogger("graph-builder");
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SPOUT)) {
                Bson myQuery = Filters.and(
                        Filters.eq(Constants.QueryProperties.QUERY, graph.getId()),
                        Filters.eq(Constants.OperatorProperties.COMPONENT,
                                node.getAttribute(Constants.OperatorProperties.COMPONENT)));

                log.info("Waiting for offsets of query {} and component {}",
                        graph.getId(),
                        node.getAttribute(Constants.OperatorProperties.COMPONENT));

                // Wait until offsets are written
                int counter = 0;
                boolean running = true;

                while (running) {
                    if (offsetCollection.find(myQuery).first() == null
                            || !offsetCollection.find(myQuery).first().containsKey(Constants.OperatorProperties.CONSUMER_POSITION)
                            || !offsetCollection.find(myQuery).first().containsKey(Constants.OperatorProperties.PRODUCER_POSITION)) {
                        Thread.sleep(1000);
                        counter += 1;
                        if (counter == 60) {
                            log.warn("Offsets of query " + graph.getId() + " have not been written fully into DB for 60, using -1 instead");
                            node.setAttribute(Constants.OperatorProperties.OFFSET, -1);
                            node.setAttribute(Constants.OperatorProperties.KAFKA_DURATION, -1);
                            node.setAttribute(Constants.QueryLabels.INGESTION_INTERVAL, -1);
                            node.setAttribute(Constants.QueryLabels.PRODUCER_INTERVAL, -1);
                            node.setAttribute(Constants.OperatorProperties.TOPIC, -1);
                            running = false;
                        }
                    } else {
                        Document offset = offsetCollection.find(myQuery).first();
                        node.setAttribute(Constants.OperatorProperties.OFFSET,
                                ((long) offset.get(Constants.OperatorProperties.PRODUCER_POSITION) - (long) offset.get(Constants.OperatorProperties.CONSUMER_POSITION)));
                        node.setAttribute(Constants.OperatorProperties.KAFKA_DURATION,
                                offset.get(Constants.OperatorProperties.KAFKA_DURATION));
                        node.setAttribute(Constants.QueryLabels.INGESTION_INTERVAL,
                                offset.get(Constants.QueryLabels.INGESTION_INTERVAL));
                        node.setAttribute(Constants.QueryLabels.PRODUCER_INTERVAL,
                                offset.get(Constants.QueryLabels.PRODUCER_INTERVAL));
                        node.setAttribute(Constants.OperatorProperties.TOPIC,
                                offset.get(Constants.OperatorProperties.TOPIC));
                        log.info("Found offsets of query {}: {}", graph.getId(), offset.toJson());
                        running = false;
                    }
                }
            }
        }
    }

    public static void readObservationsToGraph(Graph graph, MongoCollection<Document> observationColl) throws InterruptedException  {
        Logger log = LoggerFactory.getLogger("graph-builder");
        log.info("Waiting for observations of query {}", graph.getId());

        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (GraphUtils.isOperator(node)
                    && !node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SINK)
                    && !node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SPOUT)) {

                Bson query = eq("id", node.getId());
                int counter = 0;
                boolean running = true;
                while (observationColl.find(query).first() == null && running) {
                    Thread.sleep(1000);
                    counter += 1;
                    if (counter == 60) {
                        log.warn("No observation found for: {}", node.getId());
                        node.setAttribute("realSelectivity", "-1");
                        node.setAttribute("tupleWidthIn", "-1");
                        node.setAttribute("tupleWidthOut", "-1");
                        running = false;
                    }
                }
                if (observationColl.find(query).first() != null) {
                    Document observation = observationColl.find(query).first();
                    for (Map.Entry<String, Object> dc : observation.entrySet()) {
                        node.setAttribute(dc.getKey(), dc.getValue());
                    }
                }
            }
        }
    }

    public static void readLabelsToGraph(Graph graph, MongoCollection<Document> labelColl) throws InterruptedException, MongoReadException {
        Logger log = LoggerFactory.getLogger("graph-builder");
        log.info("Waiting for label of query {}", graph.getId());

        ArrayList<Double> e2eMeanList = new ArrayList<>();
        ArrayList<Double> procMeanList = new ArrayList<>();
        ArrayList<Double> tptMeanList = new ArrayList<>();
        ArrayList<Integer> countersList = new ArrayList<>();

        Node sink = GraphUtils.getSink(graph);
        int counter = 0;

        while (labelColl.count(eq(Constants.QueryProperties.QUERY, graph.getId())) != (long) Constants.NUM_SINKS) {
            // wait until labels exist
            Thread.sleep(1000);
            counter += 1;
            if (counter == 60) {
                log.warn("Label of query " + graph.getId() + " has not been written into DB for 180s, using -1 instead");
                sink.setAttribute(Constants.QueryLabels.E2E_MEAN, -1);
                sink.setAttribute(Constants.QueryLabels.PROC_MEAN,-1);
                sink.setAttribute(Constants.QueryLabels.TPT_MEAN, -1);
                sink.setAttribute(Constants.QueryLabels.COUNTER, -1);
                return;
            }
        }

        FindIterable<Document> labels = labelColl.find(eq(Constants.QueryProperties.QUERY, graph.getId()));

        if (labels != null) {
            for (Document label : labels) {
                try {
                    log.info("Label found: {}", label.toJson());
                    Double throughput = (Double) label.get(Constants.QueryLabels.TPT_MEAN);
                    Double procMean = (Double) label.get(Constants.QueryLabels.PROC_MEAN);
                    Double e2eMean = (Double) label.get(Constants.QueryLabels.E2E_MEAN);
                    int labelCounter = (int) label.get(Constants.QueryLabels.COUNTER);
                    if (throughput != 0 && labelCounter != 0 && !throughput.isInfinite()) {
                        countersList.add(labelCounter);
                        e2eMeanList.add(e2eMean);
                        procMeanList.add(procMean);
                        tptMeanList.add(throughput);
                    }
                } catch (ClassCastException exception) {
                    log.warn("Label could not be correctly parsed - are there null values?");
                }
            }

            // averaging multiple sink results
            DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
            df.setMaximumFractionDigits(6);

            // Throughput is sum of single throughputs
            Double tptMean = -1.0;
            if (!tptMeanList.isEmpty()) {
                tptMean = tptMeanList.stream().mapToDouble(val -> val).sum();
            }

            // Counter is sum of single counters
            int count = -1;
            if (!countersList.isEmpty()) {
                count = countersList.stream().mapToInt(val -> val).sum();
            }

            // Latencies are average of single latencies
            Double e2eMean = e2eMeanList.stream().mapToDouble(val -> val).average().orElse(-1.0);
            Double procMean = procMeanList.stream().mapToDouble(val -> val).average().orElse(-1.0);

            sink.setAttribute(Constants.QueryLabels.E2E_MEAN, df.format(e2eMean));
            sink.setAttribute(Constants.QueryLabels.PROC_MEAN, df.format(procMean));
            sink.setAttribute(Constants.QueryLabels.TPT_MEAN, df.format(tptMean));
            sink.setAttribute(Constants.QueryLabels.COUNTER, count);
            sink.setAttribute(Constants.QueryProperties.QUERY, labels.iterator().next().get(Constants.QueryProperties.QUERY));

        }
        log.info("Label found for query {}", graph.getId());
    }

    @Deprecated
    public static void readHWFeaturesToGraph(Graph graph, MongoCollection<Document> hardwareFeatColl, MongoCollection<Document> networkFeatColl, Config config) {
        Logger log = LoggerFactory.getLogger("graph-builder");
        // update hosts at first
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (!GraphUtils.isOperator(node)) {
                node.setAttribute("host", config.get(node.getId()));
            }
        }

        // adding hardware and network features
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (!GraphUtils.isOperator(node)) {
                log.info("Looking for offline features of {}", node.getAttribute("host"));

                Document hardwareFeatures = hardwareFeatColl.find(eq("host", node.getAttribute("host"))).first();
                if (hardwareFeatures != null) {
                    log.info("Found hardware features: {}", hardwareFeatures.toJson());
                    for (Map.Entry<String, Object> offlineFeat : hardwareFeatures.entrySet()) {
                        node.setAttribute(offlineFeat.getKey(), offlineFeat.getValue());
                    }
                }
                // add bandwidth to the edge to the successor node if applicable
                if (node.hasAttribute("host") && GraphUtils.getHostSuccessor(node) != null) {
                    Node hostSuccessor = GraphUtils.getHostSuccessor(node);
                    log.info("Found successor for node {} ", hostSuccessor.getId());
                    Document networkFeatures = networkFeatColl
                            .find(
                                    and(
                                            eq("source", node.getAttribute("host")),
                                            eq("target", hostSuccessor.getAttribute("host")))
                            )
                            .first();

                    if (networkFeatures != null) {
                        log.info("Bandwidth found between: {} and {}: {}",
                                node.getAttribute("host"),
                                hostSuccessor.getAttribute("host"),
                                networkFeatures.get("bandwidth"));
                        node.getEdgeBetween(hostSuccessor).setAttribute("bandwidth", networkFeatures.get("bandwidth"));
                    }
                }
            }
        }
    }
}

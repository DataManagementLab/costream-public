package costream.plan.executor.main;

import com.beust.jcommander.JCommander;

import costream.plan.executor.utils.EnrichedQueryGraph;
import costream.plan.executor.utils.Triple;
import costream.plan.executor.application.advertisement.AdvertisementMapper;
import costream.plan.executor.application.smartgrid.SmartGridMapper;
import costream.plan.executor.application.spikedetection.SpikeDetectionMapper;
import costream.plan.executor.kafka.KafkaSpoutOperator;
import costream.plan.executor.utils.DiskReader;
import costream.plan.executor.utils.GraphUtils;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.tuple.Tuple;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This program generates several random storm plans and executes them on a local cluster.
 */
public class PlanExecutor extends AbstractPlanExecutor {

    public static void main(String... args) {
        PlanExecutor main = new PlanExecutor();
        JCommander.newBuilder().addObject(main).build().parse(args);
        main.run();
    }

    /**
     * This is the main running method that produces and executes synthesized queries either on remote or on local cluster.
     *
     * @throws Exception Several exceptions possible when reading local IP address
     */
    private void run() {
        try {
            System.setProperty("logDir", logDir);       // later this is used in log4j2-config file
            System.out.println("PlanExecutor started. See " + logDir + " for more detailed logs");

            // initialize logger
            logger = LoggerFactory.getLogger("main");
            logger.info("Experiment started with configurations: inputDir: {}, outputDir: {}, logdir {}",
                    inputDir, outputDir, logDir);

            // Create config
            config = getClusterConfig(logDir, localExecution, inputDir, outputDir, reportMetrics);

            // Check directories and create output directory if it does not exist
            assert new File(logDir).exists();
            assert new File(inputDir).exists();
            DiskReader.createDirectory(outputDir);

            // read queries from graph
            ArrayList<Graph> allGraphs = DiskReader.readQueriesFromDir(inputDir, outputDir);
            if (allGraphs.size() == 0) {
                logger.error("Queries found at: {} are either empty or already executed.", inputDir);
                System.exit(0);
            } else {
                logger.info("{} queries found at: {} ", allGraphs.size(), inputDir);
            }

            // Convert all graphs into Storm topologies
            ArrayList<EnrichedQueryGraph> queries = new ArrayList<>();
            for (Graph graph : allGraphs) {
                StormTopology topo = convertGraphToTopo(graph, config);
                EnrichedQueryGraph queryGraph = new EnrichedQueryGraph(topo, graph, graph.getId().split("\\.")[0]);
                queries.add(queryGraph);
            }

            // update queries with meta-information like the current hostname
            String hostname = InetAddress.getLocalHost().getHostName();
            logger.info("Cluster name is: {}", hostname);
            for (EnrichedQueryGraph query : queries) {
                query.getGraph().setAttribute(Constants.QueryProperties.CLUSTER, hostname);
                // write queries back to inputDir
                DiskReader.writeSingleGraphToDir(query.getGraph(), query.getName(), inputDir);
            }

            // execute queries
            execute(queries, localExecution, config, waitingTime);
            logger.info("Program finished!");
            System.exit(0);
        } catch (Exception e) {
            logger.error("Exception", e);
            System.exit(1);
        }
    }


    public StormTopology convertGraphToTopo(Graph graph, Config config) throws ClassNotFoundException {
        StreamBuilder builder = new StreamBuilder(config);
        String queryName = graph.getId().split(".query")[0];
        ArrayList<Node> spouts = GraphUtils.getSpouts(graph);

        String mode = (String) graph.getAttribute(Constants.QueryProperties.MODE);

        if (mode.equals(Constants.QueryType.BENCHMARK.ADVERTISEMENT)) {
            convertJoinAdvertisementQuery(builder, spouts, queryName);
        } else if (mode.equals(Constants.QueryType.BENCHMARK.SPIKE_DETECTION)) {
            convertSpikeDetectionQuery(builder, spouts, queryName);
        } else if (mode.startsWith(Constants.QueryType.BENCHMARK.SMART_GRID)) {
            convertSmartGridQuery(builder, spouts, queryName);
        } else {
            switch (spouts.size()) {
                case 1:
                    convertLinearQuery(builder, spouts, queryName);
                    break;
                case 2:
                    convertTwoWayQuery(builder, spouts, queryName);
                    break;
                case 3:
                    convertThreeWayQuery(builder, spouts, queryName);
                    break;
                default:
                    throw new IllegalStateException(spouts.size() + " spouts found, which is not supported");
            }
        }
        return builder.build();
    }


    private void convertSmartGridQuery(StreamBuilder builder, ArrayList<Node> spouts, String queryName) throws ClassNotFoundException {
        Node spout = spouts.get(0);
        KafkaSpoutOperator operator = new KafkaSpoutOperator(
                Constants.Kafka.TOPIC0,
                (String) config.get("kafka.bootstrap.server"),
                (int) config.get("kafka.port"),
                (Double) spout.getAttribute("confEventRate"),
                config);

        Triple<Integer, Integer, Integer> tupleWidth = new Triple<>(
                ((Double) spout.getAttribute("numInteger")).intValue(),
                ((Double) spout.getAttribute("numDouble")).intValue(),
                ((Double) spout.getAttribute("numString")).intValue());
        operator.setTupleWidth(tupleWidth);
        HashMap<String, Object> description = operator.getDescription();
        description.put("id", spout.getAttribute("id"));

        Stream<Tuple> rawStream = builder.newStream(operator.getSpoutOperator(queryName), description);
        Stream<DataTuple> stream = rawStream.map(new SmartGridMapper(queryName), new HashMap<>());

        Node filter = GraphUtils.getOperatorSuccessor(spout);
        Stream<DataTuple> filteredStream = applyFilter(stream, filter);

        Node windowedAgg = GraphUtils.getOperatorSuccessor(filter);
        Stream<DataTuple> aggregatedStream = applyWindowedAggregation(filteredStream, windowedAgg);

        Node sink = GraphUtils.getOperatorSuccessor(windowedAgg);
        insertSinkToTopo(sink, aggregatedStream);
    }

    private void convertSpikeDetectionQuery(StreamBuilder builder, ArrayList<Node> spouts, String queryName) throws ClassNotFoundException {
        Node spout = spouts.get(0);
        KafkaSpoutOperator operator = new KafkaSpoutOperator(
                Constants.Kafka.TOPIC0,
                (String) config.get("kafka.bootstrap.server"),
                (int) config.get("kafka.port"),
                (Double) spout.getAttribute("confEventRate"),
                config);

        Triple<Integer, Integer, Integer> tupleWidth = new Triple<>(
                ((Double) spout.getAttribute("numInteger")).intValue(),
                ((Double) spout.getAttribute("numDouble")).intValue(),
                ((Double) spout.getAttribute("numString")).intValue());
        operator.setTupleWidth(tupleWidth);
        HashMap<String, Object> description = operator.getDescription();
        description.put("id", spout.getAttribute("id"));

        Stream<Tuple> rawStream = builder.newStream(operator.getSpoutOperator(queryName), description);
        Stream<DataTuple> stream = rawStream.map(new SpikeDetectionMapper(queryName), new HashMap<>());

        Node filter = GraphUtils.getOperatorSuccessor(spout);
        Stream<DataTuple> filteredStream = applyFilter(stream, filter);

        Node windowedAgg = GraphUtils.getOperatorSuccessor(filter);
        Stream<DataTuple> aggregatedStream = applyWindowedAggregation(filteredStream, windowedAgg);

        Node filter2 = GraphUtils.getOperatorSuccessor(windowedAgg);
        Stream<DataTuple> filteredStream2 = applyFilter(aggregatedStream, filter2);

        Node sink = GraphUtils.getOperatorSuccessor(filter2);
        insertSinkToTopo(sink, filteredStream2);
    }


    private void convertJoinAdvertisementQuery(StreamBuilder builder, ArrayList<Node> spouts, String queryName) throws ClassNotFoundException {
        // Clicks Stream
        Node spout1 = spouts.get(0);
        KafkaSpoutOperator kafkaSpout1 = new KafkaSpoutOperator(
                Constants.Kafka.TOPIC0,
                (String) config.get("kafka.bootstrap.server"),
                (int) config.get("kafka.port"),
                (Double) spout1.getAttribute("confEventRate"),
                config);

        Triple<Integer, Integer, Integer> tupleWidth = new Triple<>(
                ((Double) spout1.getAttribute("numInteger")).intValue(),
                ((Double) spout1.getAttribute("numDouble")).intValue(),
                ((Double) spout1.getAttribute("numString")).intValue());
        kafkaSpout1.setTupleWidth(tupleWidth);
        HashMap<String, Object> description = kafkaSpout1.getDescription();
        description.put("id", spout1.getAttribute("id"));

        Stream<Tuple> s1 = builder.newStream(kafkaSpout1.getSpoutOperator(queryName), description);
        Stream<DataTuple> convertedS1 = s1.map(new AdvertisementMapper(queryName, "ad-clicks"), new HashMap<>());

        Node filter = GraphUtils.getOperatorSuccessor(spout1);
        Stream<DataTuple> filteredConvertedS1 = applyFilter(convertedS1, filter);

        // Impression Stream
        Node spout2 = spouts.get(1);
        KafkaSpoutOperator kafkaSpout2 = new KafkaSpoutOperator(
                Constants.Kafka.TOPIC1,
                (String) config.get("kafka.bootstrap.server"),
                (int) config.get("kafka.port"),
                (Double) spout2.getAttribute("confEventRate"),
                config);

       tupleWidth = new Triple<>(
                ((Double) spout2.getAttribute("numInteger")).intValue(),
                ((Double) spout2.getAttribute("numDouble")).intValue(),
                ((Double) spout2.getAttribute("numString")).intValue());
        kafkaSpout2.setTupleWidth(tupleWidth);
        description = kafkaSpout2.getDescription();
        description.put("id", spout2.getAttribute("id"));

        Stream<Tuple> s2 = builder.newStream(kafkaSpout2.getSpoutOperator(queryName), description);
        Stream<DataTuple> convertedS2 = s2.map(new AdvertisementMapper(queryName, "ad-impressions"), new HashMap<>());

        // Joining both streams
        Node join = GraphUtils.getOperatorSuccessor(filter);
        assert join == GraphUtils.getOperatorSuccessor(spout2);
        Stream<DataTuple> joinedStream = joinStreams(filteredConvertedS1, convertedS2, join).map(Pair::getSecond);

        Node sink = GraphUtils.getOperatorSuccessor(join);
        insertSinkToTopo(sink, joinedStream);
    }

    private void convertLinearQuery(StreamBuilder builder, ArrayList<Node> spouts, String queryName) throws ClassNotFoundException {
        assert spouts.size() == 1;
        Node spout = spouts.get(0);
        Stream<DataTuple> stream = insertSpoutToTopo(builder, spout, Constants.Kafka.TOPIC0, this.config, queryName);
        while (GraphUtils.hasOperatorSuccessor(spout)) {
            spout = GraphUtils.getOperatorSuccessor(spout);
            String operatorType = (String) spout.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE);
            switch (operatorType) {
                case Constants.Operators.FILTER:
                    stream = applyFilter(stream, spout);
                    break;
                case Constants.Operators.WINDOW_AGGREGATE:
                    stream = applyWindowedAggregation(stream, spout);
                    break;
                case Constants.Operators.SINK:
                    insertSinkToTopo(spout, stream);
                    break;
                default:
                    throw new RuntimeException("Operator type " + operatorType + ", found in Graph not supported");
            }
        }
    }

    private void convertTwoWayQuery(StreamBuilder builder, ArrayList<Node> spouts, String queryName) throws ClassNotFoundException {
        assert spouts.size() == 2;
        Node node0 = spouts.get(0);
        Node node1 = spouts.get(1);

        Stream<DataTuple> stream0 = insertSpoutToTopo(builder, node0, Constants.Kafka.TOPIC0, this.config, queryName);
        Stream<DataTuple> stream1 = insertSpoutToTopo(builder, node1, Constants.Kafka.TOPIC1, this.config, queryName);

        // apply optional filter on stream0
        if (GraphUtils.isFilter(GraphUtils.getOperatorSuccessor(node0))) {
            node0 = GraphUtils.getOperatorSuccessor(node0);
            stream0 = applyFilter(stream0, node0);
        }

        // apply optional filter on stream1
        if (GraphUtils.isFilter(GraphUtils.getOperatorSuccessor(node1))) {
            node1 = GraphUtils.getOperatorSuccessor(node1);
            stream1 = applyFilter(stream1, node1);
        }

        // apply join
        node0 = GraphUtils.getOperatorSuccessor(node0);
        assert node0 == GraphUtils.getOperatorSuccessor(node1);
        Stream<DataTuple> joinedStream = joinStreams(stream0, stream1, node0).map(Pair::getSecond);

        Stream<DataTuple> eventualAggregatedStream;
        // apply optional aggregation
        if (GraphUtils.isAggregation(GraphUtils.getOperatorSuccessor(node0))) {
           node0 = GraphUtils.getOperatorSuccessor(node0);
           eventualAggregatedStream = applyAggregation(joinedStream, node0);
        } else {
            eventualAggregatedStream = joinedStream;
        }

        // apply optional filter
        if (GraphUtils.isFilter(GraphUtils.getOperatorSuccessor(node0))){
            node0 = GraphUtils.getOperatorSuccessor(node0);
            eventualAggregatedStream = applyFilter(eventualAggregatedStream, node0);
        }

        node0 = GraphUtils.getOperatorSuccessor(node0);
        insertSinkToTopo(node0, eventualAggregatedStream);
    }

    private void convertThreeWayQuery(StreamBuilder builder, ArrayList<Node> spouts, String queryName) throws ClassNotFoundException {
        assert spouts.size() == 3;
        Node node0 = spouts.get(0);
        Node node1 = spouts.get(1);
        Node node2 = spouts.get(2);

        Stream<DataTuple> stream0 = insertSpoutToTopo(builder, node0, Constants.Kafka.TOPIC0, this.config, queryName);
        Stream<DataTuple> stream1 = insertSpoutToTopo(builder, node1, Constants.Kafka.TOPIC1, this.config, queryName);
        Stream<DataTuple> stream2 = insertSpoutToTopo(builder, node2, Constants.Kafka.TOPIC2, this.config, queryName);

        // apply optional filter on stream0
        if (GraphUtils.isFilter(GraphUtils.getOperatorSuccessor(node0))) {
            node0 = GraphUtils.getOperatorSuccessor(node0);
            stream0 = applyFilter(stream0, node0);
        }

        // apply optional filter on stream1
        if (GraphUtils.isFilter(GraphUtils.getOperatorSuccessor(node1))) {
            node1 = GraphUtils.getOperatorSuccessor(node1);
            stream1 = applyFilter(stream1, node1);
        }

        // apply optional filter on stream2
        if (GraphUtils.isFilter(GraphUtils.getOperatorSuccessor(node2))) {
            node2 = GraphUtils.getOperatorSuccessor(node2);
            stream2 = applyFilter(stream2, node2);
        }

        // apply join on first both streams
        node0 = GraphUtils.getOperatorSuccessor(node0);
        assert (node0.equals(GraphUtils.getOperatorSuccessor(node1)));
        assert (node0.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.JOIN));
        Stream<DataTuple> joinedStream = joinStreams(stream0, stream1, node0).map(Pair::getSecond);

        // apply join on second and third stream
        node0 = GraphUtils.getOperatorSuccessor(node0);
        assert node0.equals(GraphUtils.getOperatorSuccessor(node2));
        Stream<DataTuple> joinedStream2 = joinStreams(joinedStream, stream2, node0).map(Pair::getSecond);

        // apply optional aggregation
        Stream<DataTuple> eventualAggregatedStream;
        if (GraphUtils.isAggregation(GraphUtils.getOperatorSuccessor(node0))) {
            node0 = GraphUtils.getOperatorSuccessor(node0);
            eventualAggregatedStream = applyAggregation(joinedStream2, node0);
        } else {
            eventualAggregatedStream = joinedStream2;
        }

        // apply optional filter
        if (GraphUtils.isFilter(GraphUtils.getOperatorSuccessor(node0))){
            node0 = GraphUtils.getOperatorSuccessor(node0);
            eventualAggregatedStream = applyFilter(eventualAggregatedStream, node0);
        }

        node0 = GraphUtils.getOperatorSuccessor(node0);
        insertSinkToTopo(node0, eventualAggregatedStream);
    }
}


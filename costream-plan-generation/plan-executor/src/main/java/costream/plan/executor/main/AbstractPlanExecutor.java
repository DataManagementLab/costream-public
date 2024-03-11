package costream.plan.executor.main;

import costream.plan.executor.operators.aggregation.AbstractAggregateIterableFunction;
import costream.plan.executor.operators.aggregation.AggregationOperatorProvider;
import costream.plan.executor.operators.aggregation.functions.AbstractAggregateFunction;
import costream.plan.executor.operators.filter.FilterOperatorProvider;
import costream.plan.executor.operators.map.functions.AbstractMapFunction;
import costream.plan.executor.utils.EnrichedQueryGraph;
import costream.plan.executor.utils.GraphUtils;
import costream.plan.executor.utils.RanGen;
import costream.plan.executor.utils.Triple;
import costream.plan.executor.operators.map.MapOperator;
import costream.plan.executor.operators.map.MapPairOperatorProvider;
import costream.plan.executor.operators.window.WindowOperatorProvider;
import costream.plan.executor.application.synthetic.SyntheticMapper;
import costream.plan.executor.kafka.KafkaSpoutOperator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;
import costream.plan.executor.kafka.KafkaRunner;
import costream.plan.executor.operators.aggregation.AggregationIterableOperatorProvider;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.PairFunction;
import org.apache.storm.streams.operations.ValueJoiner;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.thrift.TException;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.NimbusClient;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Abstract class holding general functions towards the execution of queries
 */
public class AbstractPlanExecutor {
    protected static Logger logger;
    @Parameter(names = {"--input", "-i"}, required = true, description = "the place where query plans can be found")
    String inputDir = "./plans";

    @Parameter(names = {"--output", "-o"}, required = true, description = "the place where to log files")
    String outputDir = "./plans-executed";

    @Parameter(names = {"--logdir", "-l"}, required = true, description = "the place where to log files")
    String logDir = "./logs";
    /**
     * Waiting time in ms between killing a query and running a new one
     */
    @Parameter(names = {"--waitingTime", "-w"}, description = "waiting time in between the queries in ms", validateWith = PositiveInteger.class)
    int waitingTime = 60000;
    /**
     * Local or remote execution
     **/
    @Parameter(names = {"--localExecution", "-e"}, description = "either true (for local cluster) or false (for real distributed cluster)", arity = 1)
    boolean localExecution = false;

    @Parameter(names = {"--metrics"}, description = "The path to the ssh-key to register at other machines", arity = 1)
    boolean reportMetrics = true;

    Config config;

    private static void addLocalHostMappingToConfig(Config config, Nimbus.Client client) throws TException {
        // Create a virtual host mapping - assuming a host order (e.g. cloud_lab0 is host-0).
        // This is used to correctly sustain the order of hosts to use in load-generator and Scheduler
        for (int i = 0; i < client.getClusterInfo().get_supervisors().size(); i++) {
            config.put("host-" + i, client.getClusterInfo().get_supervisors().get(i).get_host());
        }
    }

    private static void setJVMSize(Config config, EnrichedQueryGraph query) {
        int jvmSizeMb;
        try {
            jvmSizeMb = ((Double) query.getGraph().getAttribute(Constants.QueryProperties.JVM_SIZE)).intValue();
        } catch (NullPointerException e) {
            logger.info("No JVM size found for query {}, using fallback size og 10GB", query.getName());
            jvmSizeMb = 10000;
            query.getGraph().setAttribute(Constants.QueryProperties.JVM_SIZE, jvmSizeMb);
        }
        String property = "-Xmx" + jvmSizeMb + "m";
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, property);
    }

    private static void estimateExecutionEnd(ArrayList<EnrichedQueryGraph> queries, long waitingTime) {
        long sum = 0;
        for (EnrichedQueryGraph query : queries) {
            sum += (((Double) query.getGraph().getAttribute(Constants.QueryProperties.DURATION))).longValue();
        }
        sum = sum * 1000;
        sum = sum + (queries.size() * waitingTime);

        String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(sum),
                TimeUnit.MILLISECONDS.toMinutes(sum) % TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MILLISECONDS.toSeconds(sum) % TimeUnit.MINUTES.toSeconds(1));

        logger.info("Experiment will run approx. {}", hms);
        Date now = new Date();
        now.setTime(now.getTime() + sum);
        logger.info("Experiment will terminate approx. at {}", now);
    }

    /**
     * Entry point method to execute a set of queries on either a LocalCluster or a remote cluster.
     *
     * @param queries        ArrayList of Queries to execute
     * @param localExecution LocalCluster vs distributed execution
     * @param config         Storm Config holding various settings
     * @param waitingTime    Waiting time between the queries
     * @throws Exception various exceptions
     */
    public static void execute(ArrayList<EnrichedQueryGraph> queries, boolean localExecution, Config config, int waitingTime) throws Exception {
        KillOptions killOptions = new KillOptions();
        killOptions.set_wait_secs(3);
        GraphBuilder graphBuilder = new GraphBuilder(localExecution, config);
        estimateExecutionEnd(queries, waitingTime);
        if (localExecution) {
            executeSequentialOnLocalCluster(queries, config, graphBuilder, killOptions, waitingTime);
        } else {
            executeSequentialOnRemoteCluster(queries, config, graphBuilder, killOptions, waitingTime);
        }
    }

    /**
     * Execute a set of queries sequentially on a remote cluster. Before every query executions, a set of KafkaProducers
     * are created to generate the load as specified on the query.
     *
     * @param allQueries
     * @param stormConfig
     * @param graphBuilder
     * @param killOptions
     * @param waitingTime
     * @throws Exception
     */
    private static void executeSequentialOnRemoteCluster(ArrayList<EnrichedQueryGraph> allQueries,
                                                         Config stormConfig,
                                                         GraphBuilder graphBuilder,
                                                         KillOptions killOptions,
                                                         long waitingTime) throws Exception {
        // Create the client
        Nimbus.Client client =
                (Nimbus.Client) NimbusClient.getConfiguredClient(stormConfig).getClient();

        // Log existing supervisors found in the cluster
        for (Iterator<SupervisorSummary> it = client.getClusterInfo().get_supervisors_iterator(); it.hasNext(); ) {
            logger.info("Supervisor found: {}", it.next());
        }
        // add local host mapping to the config
        addLocalHostMappingToConfig(stormConfig, client);

        if (client.getClusterInfo().get_topologies_size() != 0) {
            throw new RuntimeException("There are already queries running on the cluster. Aborting");
        }


        for (int i = 0; i < allQueries.size(); i++) {
            EnrichedQueryGraph query = allQueries.get(i);
            // each query can be in a different mode => synthetic tuples or benchmark tuples from file
            String mode = (String) query.getGraph().getAttribute(Constants.QueryProperties.MODE);
            KafkaRunner kafkaRunner = new KafkaRunner(stormConfig, mode);
            stormConfig.setNumWorkers(getNumOfDistinctHosts(query.getGraph()));
            setJVMSize(stormConfig, query);
            kafkaRunner.start(query);
            long queryDuration = query.getGraph().getAttribute(Constants.QueryProperties.DURATION, Double.class).longValue() * 1000;
            logQuery(query, i, allQueries.size());
            StormSubmitter.submitTopology(query.getName(), stormConfig, query.getTopo());
            Thread.sleep(queryDuration);
            kafkaRunner.stop();
            logger.info("Killing query: {}", query.getName());
            client.killTopologyWithOpts(query.getName(), killOptions);
            try {
                graphBuilder.postExecutionUpdate(query);
            } catch (MongoReadException e) {
                logger.error("MongoReadException was thrown for query {}", query.getName());
            }
            Thread.sleep(waitingTime);
        }
    }

    /**
     * Execute a set of queries sequentially on a local cluster. Before every query executions, a set of KafkaProducers
     * are created to generate the load as specified on the query.
     *
     * @param allQueries
     * @param stormConfig
     * @param graphBuilder
     * @param killOptions
     * @param waitingTime
     * @throws Exception
     */
    private static void executeSequentialOnLocalCluster(ArrayList<EnrichedQueryGraph> allQueries,
                                                        Config stormConfig,
                                                        GraphBuilder graphBuilder,
                                                        KillOptions killOptions,
                                                        long waitingTime) throws Exception {
        LocalCluster client = new LocalCluster();
        for (Iterator<SupervisorSummary> it = client.getClusterInfo().get_supervisors_iterator(); it.hasNext(); ) {
            logger.info("Supervisor found: {}", it.next());
        }

        for (int i = 0; i < allQueries.size(); i++) {
            EnrichedQueryGraph query = allQueries.get(i);
            // each query can be in a different mode => synthetic tuples or benchmark tuples from file
            String mode = (String) query.getGraph().getAttribute(Constants.QueryProperties.MODE);
            KafkaRunner kafkaRunner = new KafkaRunner(stormConfig, mode);
            stormConfig.setNumWorkers(getNumOfDistinctHosts(query.getGraph()));
            setJVMSize(stormConfig, query);
            kafkaRunner.start(query);
            long queryDuration = query.getGraph().getAttribute(Constants.QueryProperties.DURATION, Double.class).longValue() * 1000;
            logQuery(query, i, allQueries.size());
            client.submitTopology(query.getName(), stormConfig, query.getTopo());
            Thread.sleep(queryDuration);
            kafkaRunner.stop();
            client.killTopologyWithOpts(query.getName(), killOptions);
            Thread.sleep(10000);
            graphBuilder.postExecutionUpdate(query);
            Thread.sleep(waitingTime);
        }
        client.shutdown();
    }


    private static void logQuery(EnrichedQueryGraph query, int index, int end) {
        logger.info("Execute query {} ({}/{}) with the following parameters:", query.getName(), index + 1, end);
        for (Iterator<String> it = query.getGraph().attributeKeys().iterator(); it.hasNext(); ) {
            String key = it.next();
            logger.info("--{}: {}", key, query.getGraph().getAttribute(key));
        }
    }

    private static int getNumOfDistinctHosts(Graph graph) {
        Set<String> distinctHosts = new HashSet<>();
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (GraphUtils.isOperator(node)) {
                Node host = GraphUtils.getHostSuccessor(node);
                assert host != null;
                distinctHosts.add(host.getId());
            }
        }
        return distinctHosts.size();
    }

    /**
     * Apply an aggregation on a windowed stream
     *
     * @param input Input stream to aggregate on
     * @param node  GraphNode containing information about aggregation
     * @return aggregated Stream
     * @throws ClassNotFoundException exception
     */
    static Stream<DataTuple> applyAggregation(Stream<DataTuple> input, Node node) throws ClassNotFoundException {
        HashMap<String, Object> description = new HashMap<>();
        description.put("id", node.getAttribute("id"));
        description.put(Constants.OperatorProperties.OPERATOR_TYPE, Constants.Operators.AGGREGATE);
        if (node.getAttribute("groupByClass").equals("null")) {
            return input.
                    aggregate(getAggregator(node), description);
        } else {
            return input
                    .mapToPair(getMapToPair(node))
                    .groupByKey(description)
                    .aggregate(getIterableAggregator(node))
                    .map(Pair::getSecond);
        }
    }

    /**
     * The ValueJoiner describes, how to join two tuples. In this case, both DataTuples would be concatenated class-wise.
     * E.g. the integers of Tuple1 would be concatenated to the integers of Tuple2 and so on.
     * The oldest timestamp is kept for measuring the E2E-latency and the youngest for measuring the processing latency
     *
     * @return A ValueJoiner
     */
    static ValueJoiner<DataTuple, DataTuple, DataTuple> getValueJoiner() {
        return (value1, value2) -> {
            String queryName1 = value1.getQueryName();
            String queryName2 = value2.getQueryName();
            assert queryName1.equals(queryName2);
            long e2eTimestamp =
                    Math.min(value1.getE2ETimestamp(), value2.getE2ETimestamp());
            long processingTimestamp =
                    Math.min(value1.getProcessingTimestamp(), value2.getProcessingTimestamp());
            TupleContent content1 = value1.getTupleContent();
            TupleContent content2 = value2.getTupleContent();
            TupleContent combined = new TupleContent();

            // do join
            for (Map.Entry<Class<?>, ArrayList<Object>> entry : content1.entrySet()) {
                ArrayList<Object> combinedList = content1.get(entry.getKey());
                combinedList.addAll(content2.get(entry.getKey()));
                combined.put(entry.getKey(), combinedList);
            }

            return new DataTuple(combined, e2eTimestamp, processingTimestamp, queryName1);
        };
    }


    /**
     * Inserting a Sink to a given stream. This is implemented by the LoggerBolt that logs the resulting labels
     *
     * @param node   GraphNode containing sink information (which is not too much, as the sink has no other function)
     * @param stream resulting stream
     */
    public static void insertSinkToTopo(Node node, Stream<?> stream) {
        HashMap<String, Object> description = new HashMap<>();
        description.put("id", node.getAttribute("id"));
        assert node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SINK);
        stream.to(new LoggerBolt(), Constants.NUM_SINKS, description);
    }

    /**
     * Applying a filter on a stream as specified in the node
     *
     * @param input incoming stream
     * @param node  GraphNode that contains filter information
     * @return outgoing stream
     * @throws ClassNotFoundException
     */
    static Stream<DataTuple> applyFilter(Stream<DataTuple> input, Node node) throws ClassNotFoundException {
        Class<?> klass = Class.forName("java.lang." + node.getAttribute("filterClass"));
        BiFunction<Object, Object, Boolean> func = (BiFunction<Object, Object, Boolean> & Serializable) new FilterOperatorProvider().getFilterFunction((String) node.getAttribute("filterFunction"), klass);
        Object literal;
        if (klass == Integer.class) {
            literal = ((Double) node.getAttribute("literal")).intValue();
        } else if (klass == String.class) {
            literal = String.valueOf(node.getAttribute("literal"));
        } else if (klass == Double.class) {
            literal = Double.valueOf(String.valueOf(node.getAttribute("literal")));
        } else throw new RuntimeException("Class found in node not defined yet");
        HashMap<String, Object> description = new HashMap<>();
        description.put("id", node.getAttribute("id"));
        return input.filter(x -> func.apply(x.getTupleValue(klass, RanGen.randInt(0, x.getTupleContent().get(klass).size() - 1)), literal), description);
    }

    /**
     * Get a Window out of a node
     *
     * @param node A GraphNode
     * @return Window
     */
    static Window<?, ?> getWindow(Node node) {
        double windowLength = (double) node.getAttribute("windowLength");
        double slidingLength = (double) node.getAttribute("slidingLength");
        String windowPolicy = (String) node.getAttribute("windowPolicy");
        String windowType = (String) node.getAttribute("windowType");
        return new WindowOperatorProvider().getWindow((int) windowLength, (int) slidingLength, windowPolicy, windowType);
    }

    /**
     * Get aggregation function out of a node
     *
     * @param node GraphNode containing aggregation function
     * @return aggregation function
     * @throws ClassNotFoundException
     */
    static AbstractAggregateFunction getAggregator(Node node) throws ClassNotFoundException {
        String aggFunction = (String) node.getAttribute("aggFunction");
        Class<?> aggClass = Class.forName("java.lang." + node.getAttribute("aggClass"));
        return new AggregationOperatorProvider().getAggregator(aggFunction, aggClass);
    }

    /**
     * Get an iterable aggregation functions out of a GraphNode
     *
     * @param node GraphNode containing iterable aggregation function
     * @return iterable aggregation function
     * @throws ClassNotFoundException
     */
    static AbstractAggregateIterableFunction getIterableAggregator(Node node) throws ClassNotFoundException {
        String aggFunction = (String) node.getAttribute("aggFunction");
        Class<?> aggClass = Class.forName("java.lang." + node.getAttribute("aggClass"));
        return new AggregationIterableOperatorProvider().getIterableAggregator(aggFunction, aggClass);
    }

    static PairFunction<DataTuple, Object, DataTuple> getMapToPair(Node node) throws ClassNotFoundException {
        Class<?> aggClass;
        if (node.hasAttribute("groupByClass")) {
            aggClass = Class.forName("java.lang." + node.getAttribute("groupByClass"));
        } else if (node.hasAttribute("joinKeyClass")) {
            aggClass = Class.forName("java.lang." + node.getAttribute("joinKeyClass"));
        } else {
            throw new IllegalArgumentException("Node has an invalid attribute set for mapToPair");
        }
        return new MapPairOperatorProvider().getMapToPair(aggClass);
    }

    /**
     * Apply a windowed aggregation on a stream
     *
     * @param input incoming dataStram
     * @param node  GraphNode containing window and aggregation information
     * @return aggregated stream
     * @throws ClassNotFoundException exception
     */
    static Stream<DataTuple> applyWindowedAggregation(Stream<DataTuple> input, Node node) throws ClassNotFoundException {
        HashMap<String, Object> description = new HashMap<>();
        description.put("id", node.getAttribute("id"));
        if (node.getAttribute("groupByClass").equals("null")) {
            return input
                    .window(getWindow(node))
                    .aggregate(getAggregator(node), description);
        } else {
            return input
                    .window(getWindow(node))
                    .mapToPair(getMapToPair(node))
                    .groupByKey(description)
                    .aggregate(getIterableAggregator(node))
                    .map(Pair::getSecond);
        }
    }

    /**
     * Inserting a KafkaSpout to the topology. Note that a KafkaMapper is applied as well to map the incoming Tuples to
     * DataTuples
     *
     * @param builder   A Storm StreamBuilder that creates the new stream
     * @param spout     A GraphNode that contains the spout information
     * @param topic     The topic to Kafka published on and the Spout listens on
     * @param config    Storm configurations holding various information
     * @param queryName Name of the query to execute for logging reasons
     * @return newly created stream
     */
    public static Stream<DataTuple> insertSpoutToTopo(StreamBuilder builder,
                                                      Node spout,
                                                      String topic,
                                                      Config config,
                                                      String queryName) {
        String bootstrapServer = (String) config.get("kafka.bootstrap.server");
        int port = (int) config.get("kafka.port");
        KafkaSpoutOperator operator = new KafkaSpoutOperator(
                topic, bootstrapServer, port, (Double) spout.getAttribute("confEventRate"), config);
        Triple<Integer, Integer, Integer> tupleWidth = new Triple<>(
                ((Double) spout.getAttribute("numInteger")).intValue(),
                ((Double) spout.getAttribute("numDouble")).intValue(),
                ((Double) spout.getAttribute("numString")).intValue());
        operator.setTupleWidth(tupleWidth);
        HashMap<String, Object> description = operator.getDescription();
        description.put("id", spout.getAttribute("id"));
        Stream<Tuple> s1 = builder.newStream(operator.getSpoutOperator(queryName), description);
        MapOperator map = new MapOperator(new SyntheticMapper(tupleWidth, queryName));
        map.getDescription().put(Constants.QueryProperties.QUERY, queryName);
        return s1.map((AbstractMapFunction<Tuple, DataTuple>) map.getFunction(), map.getDescription());
    }

    /**
     * Joins two streams as specified in the GraphNode. Both streams need to be mapped as mapToPair function first.
     * The node needs to contain a window to properly join
     *
     * @param left  left stream to join
     * @param right right stream to join
     * @param node  node containing join information
     * @return PairStream, e.g. joined Stream
     * @throws ClassNotFoundException exception
     */
    static PairStream<Object,DataTuple> joinStreams(Stream<DataTuple> left, Stream<DataTuple> right, Node node) throws ClassNotFoundException {
        HashMap<String, Object> description = new HashMap<>();
        description.put("id", node.getAttribute("id"));
        description.put(Constants.OperatorProperties.OPERATOR_TYPE, Constants.Operators.JOIN);
        PairStream<Object, DataTuple> left_pair = left.mapToPair(getMapToPair(node));
        PairStream<Object, DataTuple> right_pair = right.mapToPair(getMapToPair(node));
        return left_pair
                .window(getWindow(node))
                .join(right_pair, getValueJoiner(), description);
    }

    /**
     * Loading storm configurations and adds custom configs to that
     *
     * @return Custom configuration for a remote storm cluster
     */
    public static Config getClusterConfig(String logPath,
                                          boolean localExecution,
                                          String inputPath,
                                          String outputPath,
                                          boolean metrics) throws IOException {
        String hostname = new BufferedReader(
                new InputStreamReader(Runtime.getRuntime().exec("hostname -s ").getInputStream()))
                .readLine();

        Config config = new Config();

        // disabling debug mode
        config.put("topology.debug", false);

        // setting paths to config as they are required in various locations later
        config.put("log.path", logPath);
        config.put("input.path", inputPath);
        config.put("output.path", outputPath);

        // Set kafka port
        config.put("kafka.port", 9092);

        // Disable acking
        config.setNumAckers(0);

        // required to have large windows
        config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 600);
        config.put(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, 600);

        // Serialization settings
        config.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);

        if (metrics) {
            // Setting CSV reporter to additionally get Storm metrics
            Map<String, String> reporterConf = new HashMap<>();
            reporterConf.put("class", "org.apache.storm.metrics2.reporters.CsvStormReporter");
            reporterConf.put("csv.log.dir", logPath + "/csv");
            config.put(Config.TOPOLOGY_METRICS_REPORTERS, Collections.singletonList(reporterConf));
        }

        if (localExecution) {
            config.put("storm.cluster.mode", "local");
            config.put("placement.path", logPath + "/placement.log");
            config.put("grouping.path", logPath + "/grouping.log");
            config.put("observation.path", logPath + "/observation.log");
            config.put("labels.path", logPath + "/labels.log");
            config.put("offset.path", logPath + "/offsets.log");
            config.put("kafka.bootstrap.server", "127.0.0.1");

            // Create labels file
            File file = new File((String) config.get("labels.path"));
            file.createNewFile();
        }

        if (!localExecution) { // remote execution
            config.put("storm.cluster.mode", "distributed");
            config.put("storm.local.dir", "/tmp/storm");
            config.put("nimbus.thrift.port", 6627);
            config.put("storm.zookeeper.port", 2181);
            config.put("topology.builtin.metrics.bucket.size.secs", 10);
            config.put("drpc.request.timeout.secs", 1600);
            config.put("supervisor.worker.start.timeout.secs", 1200);
            config.put("nimbus.supervisor.timeout.secs", 1200);
            config.put("nimbus.task.launch.secs", 1200);
            config.put("task.heartbeat.frequency.secs", null);
            config.put(Constants.MongoConstants.MONGO_PORT, 27017);
            config.put(Constants.MongoConstants.MONGO_ADDRESS, hostname);
            config.put("mongo.database", "dsps");
            config.put("mongo.collection.labels", "query_labels");
            config.put("mongo.collection.observations", "query_observations");
            config.put("mongo.collection.placement", "query_placement");
            config.put("mongo.collection.graphs", "query_graphs");
            config.put("mongo.collection.grouping", "query_grouping");
            config.put("mongo.collection.offsets", "query_offsets");
            config.put("mongo.collection.hw.params", "hw_params");
            config.put("mongo.collection.nw.params", "nw_params");
            config.put("kafka.bootstrap.server", hostname);
            config.put("nimbus.seeds", Collections.singletonList(hostname));
            config.put("storm.zookeeper.servers", Collections.singletonList(hostname));
        }
        return config;
    }
}

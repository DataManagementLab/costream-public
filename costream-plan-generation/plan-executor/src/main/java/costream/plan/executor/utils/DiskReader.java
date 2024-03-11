package costream.plan.executor.utils;

import costream.plan.executor.main.Constants;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSinkDOT;
import org.graphstream.stream.file.FileSourceDOT;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class DiskReader {
    public static Set<String> listFilesInDir(String dir) {
        try {
            Objects.requireNonNull(new File(dir).listFiles());
        } catch (NullPointerException e) {
            System.out.printf("Directory %s is empty or invalid \n", dir);
        }
        return java.util.stream.Stream.of(Objects.requireNonNull(new File(dir).listFiles()))
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .filter(relativePath -> relativePath.endsWith(".query"))
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * This reads all graphs from a directory except from these that are
     * already in the output directory and thus executed.
     *
     * @param inputDir to read out queries from
     * @param outputDir to check if there already executed queries
     * @return List of compiled graphs
     * @throws IOException
     *
     */
    public static ArrayList<Graph> readQueriesFromDir(String inputDir, String outputDir) throws IOException {
        ArrayList<Graph> graphs = new ArrayList<>();
        FileSourceDOT fs = new FileSourceDOT();
        Graph graph;
        Set<String> executedQueries = listFilesInDir(outputDir);
        for (String queryName : listFilesInDir(inputDir)) {
            if (!executedQueries.contains(queryName)) {
                graph = new SingleGraph(queryName);
                fs.clearSinks();
                fs.addSink(graph);
                fs.readAll(inputDir + "/" + queryName);
                graphs.add(graph);
            }
        }
        return graphs;
    }

    /**
     * This reads a single graph from disk by looking for it in a given directory
     *
     * @param graphName Graph to find
     * @param dir       Directory to inspect
     * @return Graph that is returned
     */
    public static Graph readGraphFromDir(String graphName, String dir) {
        Graph graph = new SingleGraph(graphName);
        graphName = graphName + ".query";
        FileSourceDOT fs = new FileSourceDOT();
        for (String queryName : listFilesInDir(dir)) {
            if (queryName.equals(graphName)) {
                fs.addSink(graph);
                try {
                    fs.readAll(dir + "/" + queryName);
                    return graph;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }

    public static void writeSingleGraphToDir(Graph graph, String graphName, String dir) throws IOException {
        // Create output directory if it does not exist
        File directory = new File(dir);
        if (!directory.exists()) {
            boolean created = directory.mkdir();
            if (!created) {
                throw new IOException("Directory could not be created");
            }
        }

        FileSinkDOT fs = new FileSinkDOT();
        fs.setDirected(true);
        fs.writeAll(graph, dir + "/" + graphName + ".query");
    }

    /**
     * This reads observations that are stored on an observation file on the disk and injects them
     * to the corresponding query graph nodes by the id.
     *
     * @param graph Graph to add observations to
     * @param path  Path to the observation file
     * @throws IOException
     * @throws ParseException
     */
    public static void readObservationsFromFileToGraph(Graph graph, String path) throws IOException, ParseException {
        Logger log = LoggerFactory.getLogger("graph-builder");
        log.info("Reading observations from file {} to graph: {} ", path, graph.getId());
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(path));
        JSONParser parser = new JSONParser();
        while ((line = reader.readLine()) != null) {
            JSONObject observation = (JSONObject) parser.parse(line);
            String id = (String) observation.get("id");
            if (graph.getNode(id) != null) {
                for (String key : (Iterable<String>) observation.keySet()) {
                    graph.getNode(id).setAttribute(key, observation.get(key));
                }
            }
        }
    }

    /**
     * This reads a file that contains written map-Objects. An "id"-Field is required for each operator and the information
     * is merged into the graph object
     *
     * @param path
     * @throws IOException
     */
    public static void addComponentsFromFileToGraph(Graph graph, String path) throws IOException, ParseException {
        Logger log = LoggerFactory.getLogger("graph-builder");
        log.info("Reading components from file {} to graph: {} ", path, graph.getId());
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(path));
        JSONParser parser = new JSONParser();
        while ((line = reader.readLine()) != null) {
            JSONObject object = (JSONObject) parser.parse(line);
            String id = (String) object.get("operator");
            if (graph.getNode(id) != null) {
                graph.getNode(id).setAttribute(Constants.OperatorProperties.COMPONENT, object.get(Constants.OperatorProperties.COMPONENT));
            }
        }
    }

    public static void addOffsetsFromFileToGraph(Graph graph, String path) throws IOException, ParseException, InterruptedException {
        Logger log = LoggerFactory.getLogger("graph-builder");
        log.info("Reading offsets from file {} to graph: {} ", path, graph.getId());
        String line;
        Thread.sleep(5000);
        BufferedReader reader;
        JSONParser parser = new JSONParser();

        if (graph.getId() == null) {
            throw new RuntimeException("Graph has no ID");
        }

        // read consumers
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            reader = new BufferedReader(new FileReader(path));
            if (node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SPOUT)) {
                while ((line = reader.readLine()) != null) {
                    JSONObject offset = (JSONObject) parser.parse(line);
                    String queryId = (String) offset.get(Constants.QueryProperties.QUERY);
                    String component = (String) node.getAttribute(Constants.OperatorProperties.COMPONENT);
                    if (graph.getId().equals(queryId)
                            && offset.containsKey("consumer")
                            && offset.get(Constants.OperatorProperties.COMPONENT).equals(component)) {
                        node.setAttribute(Constants.QueryLabels.INGESTION_INTERVAL,
                                offset.get(Constants.QueryLabels.INGESTION_INTERVAL));
                        node.setAttribute(Constants.QueryLabels.PRODUCER_INTERVAL,
                                offset.get(Constants.QueryLabels.PRODUCER_INTERVAL));

                        JSONObject consumer = (JSONObject) offset.get(Constants.OperatorProperties.CONSUMER_POSITION);
                        long consumerPos = Long.parseLong((String) consumer.get("$numberLong"));
                        node.setAttribute(Constants.OperatorProperties.CONSUMER_POSITION, consumerPos);
                        node.setAttribute(Constants.OperatorProperties.TOPIC,
                                offset.get(Constants.OperatorProperties.TOPIC));
                    }
                }
            }
        }

        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext();) {
            Node node = it.next();
            reader = new BufferedReader(new FileReader(path));
            if (node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SPOUT)) {
                while ((line = reader.readLine()) != null) {
                    JSONObject offset = (JSONObject) parser.parse(line);
                    String queryId = (String) offset.get(Constants.QueryProperties.QUERY);
                    String topic = (String) node.getAttribute(Constants.OperatorProperties.TOPIC);
                    if (graph.getId().equals(queryId)
                            && offset.containsKey("producer")
                            && offset.get(Constants.OperatorProperties.TOPIC).equals(topic)) {

                        long producerPosition = Long.parseLong((String) ((JSONObject)
                                offset.get(Constants.OperatorProperties.PRODUCER_POSITION)).get("$numberLong"));
                        long kafkaDuration = Long.parseLong((String) ((JSONObject)
                                offset.get(Constants.OperatorProperties.KAFKA_DURATION)).get("$numberLong"));

                        node.setAttribute(Constants.OperatorProperties.KAFKA_DURATION,
                                kafkaDuration);
                        node.setAttribute(Constants.OperatorProperties.PRODUCER_POSITION,
                                producerPosition);

                        long consumerPosition = (long) node.getAttribute(Constants.OperatorProperties.CONSUMER_POSITION);
                        long diff = producerPosition - consumerPosition;
                        node.setAttribute(Constants.OperatorProperties.OFFSET, diff);
                    }
                }
            }
        }
    }

    public static void addLabelsFromFileToGraph(Graph graph, String path) throws IOException, ParseException, InterruptedException {
        Logger log = LoggerFactory.getLogger("graph-builder");
        log.info("Reading labels from file {} to graph: {} ", path, graph.getId());

        ArrayList<Double> e2eMean = new ArrayList<>();
        ArrayList<Double> procMean = new ArrayList<>();
        ArrayList<Double> tptMean = new ArrayList<>();
        ArrayList<Integer> counters = new ArrayList<>();

        Node sink = GraphUtils.getSink(graph);
        int counter = 0;

        String line;
        BufferedReader reader = new BufferedReader(new FileReader(path));
        JSONParser parser = new JSONParser();

        while (e2eMean.size() == 0) {
            while ((line = reader.readLine()) != null) {
                JSONObject label = (JSONObject) parser.parse(line);
                log.info("Label found: {}", label.toJSONString());
                String id = (String) label.get(Constants.QueryProperties.QUERY);
                if (graph.getId() != null && graph.getId().equals(id)) {
                    try {
                        counters.add((int) (long) label.get(Constants.QueryLabels.COUNTER));
                        e2eMean.add((Double) label.get(Constants.QueryLabels.E2E_MEAN));
                        procMean.add((Double) label.get(Constants.QueryLabels.PROC_MEAN));
                        tptMean.add((Double) label.get(Constants.QueryLabels.TPT_MEAN));
                    } catch (ClassCastException exception) {
                        log.warn("Label could not be correctly parsed - are there null values?");
                    }
                }
            }
        }

        // averaging multiple sink results
        sink.setAttribute(Constants.QueryLabels.E2E_MEAN, e2eMean.stream().mapToDouble(val -> val).average().orElse(0.0));
        sink.setAttribute(Constants.QueryLabels.PROC_MEAN, procMean.stream().mapToDouble(val -> val).average().orElse(0.0));
        sink.setAttribute(Constants.QueryLabels.TPT_MEAN, tptMean.stream().mapToDouble(val -> val).sum());
        sink.setAttribute(Constants.QueryLabels.COUNTER, counters.stream().mapToInt(val -> val).sum());
        sink.setAttribute(Constants.QueryProperties.QUERY, graph.getId());
        Thread.sleep(1000);
    }

    public static void createDirectory(String path) throws IOException {
        File outDir = new File(path);
        if (!outDir.exists()) {
            boolean created = outDir.mkdirs();
            if (!created) {
                throw new IOException("Directory " + path + " could not be created");
            }
        }
    }
}

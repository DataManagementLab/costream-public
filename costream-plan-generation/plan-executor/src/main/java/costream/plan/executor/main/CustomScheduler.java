package costream.plan.executor.main;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import costream.plan.executor.utils.DiskReader;
import costream.plan.executor.utils.GraphUtils;
import costream.plan.executor.utils.MongoClientProvider;
import costream.plan.executor.utils.MongoReader;
import costream.plan.executor.utils.Triple;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.utils.Utils;
import org.bson.Document;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This scheduler reads out a given placement for a graph from disk/database.
 * It assigns the operators correspondingly on the hosts
 * The graph that is read contains virtual hosts in the form of host-1, host-2, etc.
 * These are then replaced with the real hosts/slots that are provided by the cluster.
 */
public class CustomScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger("scheduler");
    private Map<String, Object> conf;

    @Override
    public void prepare(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        this.conf = conf;
    }

    private void addGroupingsToGraph(Graph graph) throws IOException, ParseException {
        // read groupings to the graph representation, currently coming from file and created in StreamBuilder
        if (this.conf.get("storm.cluster.mode").equals("local")) {
            DiskReader.addComponentsFromFileToGraph(graph, (String) this.conf.get("grouping.path"));
        } else {
            MongoClient mongoClient = MongoClientProvider.getMongoClient(conf);
            MongoCollection<Document> groupings = mongoClient
                    .getDatabase((String) conf.get("mongo.database"))
                    .getCollection((String) conf.get("mongo.collection.grouping"));
            MongoReader.addComponentsFromDbToGraph(graph, groupings);
            mongoClient.close();
        }
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        for (TopologyDetails topology : cluster.needsSchedulingTopologies()) {
            this.conf = topology.getConf();
            String graphDir = (String) this.conf.get("input.path");
            String topologyId = topology.getId();
            LOG.info("Scheduling for query {} by reading from dir {}", topology.getName(), graphDir);

            Graph graph = DiskReader.readGraphFromDir(topology.getName(), graphDir);
            if (graph == null) {
                throw new RuntimeException("Graph " + topology.getName() + "at dir: "+ graphDir + " is null!");
            }

            // add groupings to graph and schedule query
            ArrayList<Triple<WorkerSlot, List<ExecutorDetails>, WorkerResources>> placement;
            try {
                addGroupingsToGraph(graph);
                placement = scheduleTopology(topology, cluster, graph);
            } catch (IOException | ParseException | InterruptedException e) {
                throw new RuntimeException(e);
            }

            // assign executors to slots from given assignment
            for (Triple<WorkerSlot, List<ExecutorDetails>, WorkerResources> entry : placement) {
                WorkerSlot nodePort = entry.getFirst();
                List<ExecutorDetails> executors = entry.getSecond();
                WorkerResources resources = entry.getThird();
                cluster.assignWithCustomWorkerResources(nodePort, topologyId, executors, resources);
            }

            // check if scheduling was successful
            if (cluster.needsScheduling(topology)) {
                LOG.info("Topology not properly assigned.");
                LOG.info("Unassigned executors: {}", cluster.getUnassignedExecutors(topology).toString());
                LOG.info("Required number of workers: {}", topology.getNumWorkers());
                LOG.info("Assigned number of workers {}", cluster.getAssignedNumWorkers(topology));
                throw new RuntimeException("Topology not properly assigned");
            }
            // update graph on disk
            try {
                DiskReader.writeSingleGraphToDir(graph, topology.getName(), graphDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * Takes a storm topology, a cluster object and a query graphs and assigns corresponding operators to hosts
     * @param topology StormTopology that is to be scheduled
     * @param cluster Cluster object
     * @param graph Query graph
     * @return A list of (WorkerSlot, ExecutorDetails and WorkerResources) is returned as these mark related objects
     *      * The WorkerSlot is the slot to locate the Executors on with a given set of limited resources
     * @throws InterruptedException
     */
    private ArrayList<Triple<WorkerSlot, List<ExecutorDetails>, WorkerResources>> scheduleTopology(
            TopologyDetails topology,
            Cluster cluster,
            Graph graph) throws InterruptedException {
        List<WorkerSlot> allSlots = cluster.getAvailableSlots();
        LOG.info("Available Slots: " + allSlots);

        int secondsWaitingForSlots = 0;
        int requiredHosts = GraphUtils.getNumOfHosts(graph);
        while (allSlots.size() < requiredHosts) {
            Thread.sleep(1000);
            secondsWaitingForSlots += 1;
            if (secondsWaitingForSlots == 120) {
                throw new RuntimeException(
                        requiredHosts
                        + " required slots for query "
                        + graph.getId()
                        + " are not available in CustomScheduler within "
                        + secondsWaitingForSlots
                        + "s");
            }
        }

        // go over graph and collect information for every node in order to read out the correct placements
        SchedulerHostRegistry hostRegistry = new SchedulerHostRegistry(cluster, topology, conf);
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (GraphUtils.isOperator(node)) {
                hostRegistry.add(node);
            }
        }

        HashMap<String, List<ExecutorDetails>> componentToExecutor = Utils.reverseMap(topology.getExecutorToComponent());
        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<>();

        // manually place ackers in a round-robin way - do not add resources here
        List<ExecutorDetails> ackers = componentToExecutor.get("__acker");
        if (ackers != null) {
            LOG.info(ackers.size() + " ackers found to distribute for query " + topology.getName());
            for (int i = 0; i < ackers.size(); i++) {
                reassignment.put(ackers.get(i), allSlots.get(i));
            }
        }

        ArrayList<SchedulerHost> hosts = hostRegistry.getHosts();
        for (SchedulerHost host : hosts) {
            WorkerSlot desiredWorkerSlot = host.getWorkerSlot();
            for (WorkerSlot slot : allSlots) {
                if (slot.getNodeId().equals(desiredWorkerSlot.getNodeId())
                        && slot.getPort() == desiredWorkerSlot.getPort()) {
                    for (ExecutorDetails executorDetail : host.getExecutorDetails()) {
                        reassignment.put(executorDetail, slot);
                    }
                }
            }
        }

        // optionally place LogingMetricsConsumer on first slot, if metrics are enabled
        if (topology.getConf().get("topology.metrics.consumer.register") != null) {
            LOG.info(("Found a metrics to consumer to locate"));
            List<ExecutorDetails> metricComponents =
                    componentToExecutor.get("__metrics_org.apache.storm.metric.LoggingMetricsConsumer");
            assert metricComponents.size() == 1;
            ExecutorDetails metricComponent = metricComponents.get(0);
            reassignment.put(metricComponent, allSlots.get(0));
        }

        HashMap<WorkerSlot, List<ExecutorDetails>> tmp = Utils.reverseMap(reassignment);

        // adding worker resources
        ArrayList<Triple<WorkerSlot, List<ExecutorDetails>, WorkerResources>> out = new ArrayList<>();
        for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : tmp.entrySet()) {
            out.add(new Triple<>(entry.getKey(), entry.getValue(), hostRegistry.getWorkerResourcesBySlot(entry.getKey())));
        }
        return out;
    }

    @Override
    public Map<?,?> config() {
        return null;
    }

    @Override
    public void cleanup() {
        IScheduler.super.cleanup();
    }

    static class SchedulerHostRegistry {
        private final Cluster cluster;
        private final HashMap<String, WorkerSlot> hostMapping;
        private final ArrayList<SchedulerHost> schedulerHosts;
        private final HashMap<String, List<ExecutorDetails>> componentToExecutor;
        private final Map<String, Object> config;

        public SchedulerHostRegistry(Cluster cluster, TopologyDetails topology, Map<String, Object> config) {
            this.componentToExecutor = Utils.reverseMap(topology.getExecutorToComponent());
            this.cluster = cluster;
            this.schedulerHosts = new ArrayList<>();
            //create mapping of virtual hosts to real hostnames in round-robin manner, so that host-0 is first slot etc.
            this.hostMapping = new HashMap<>();
            for (int i = 0; i < cluster.getAvailableSlots().size(); i++) {
                hostMapping.put("host-" + i, cluster.getAvailableSlots().get(i));
            }
            this.config = config;
        }


        /**
         * This function parses a placement by reading all relevant information out of the graph. A graph node containing
         * the operator that is to be placed is passed and the corresponding host information is extracted and added
         * to the schedulerHost-list that is later used to actually assign the operators.
         * @param operatorNode
         */
        public void add(Node operatorNode) {
            // Get operator type
            String operatorType = (String) operatorNode.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE);

            // Get host
            Node hostNode = GraphUtils.getHostSuccessor(operatorNode);
            if (hostNode == null || GraphUtils.isOperator(hostNode)) {
                throw new RuntimeException("Operator "
                        + operatorNode.getId()
                        + " has either no host to locate or host is operator");
            }

            // Get physical operator name like bolt1, spout2, etc.
            String physicalOperatorName = (String) operatorNode.getAttribute(Constants.OperatorProperties.COMPONENT);

            // Get the correct slot which is later used for actual placement
            // in local mode just use the host mapping (round-robin)
            // in cluster mode, use the defined host order that is stored in config.
            // That host order is stored in a fixed manner in the config to be consistent over many queries and
            // eventual outages

            WorkerSlot workerSlot;
            if (this.config.get("storm.cluster.mode").equals("local")) {
                workerSlot = hostMapping.get(hostNode.getId());
            } else {
                String hostAddress = (String) config.get(hostNode.getId());
                List<SupervisorDetails> supervisorDetails = cluster.getSupervisorsByHost(hostAddress);
                LOG.info("Looking for slot for host address: {} at host id: {} with supervisors: {}", hostAddress, hostNode.getId(), supervisorDetails);
                if (supervisorDetails == null || supervisorDetails.size() == 0) {
                    throw new RuntimeException("No supervisor found for host " + hostAddress);
                }
                List<WorkerSlot> slots = new ArrayList<>();
                for (SupervisorDetails details : supervisorDetails) {
                    if (details.getHost().equals(hostAddress) && cluster.getAvailableSlots(details) != null){
                        slots = cluster.getAvailableSlots(details);
                    }
                }
                if (slots.size() != 1) {
                    LOG.error("Slot Size of supervisor " + supervisorDetails + " is not equals 1");
                    LOG.error("All Worker slots: {} ", slots);
                    throw new RuntimeException("Slot Size of supervisor " + supervisorDetails + " is not equals 1");
                }
                workerSlot = slots.get(0);
            }
            if (workerSlot == null) {
                throw new RuntimeException("No corresponding worker slot could be found for operator");
            }

            // Get virtual hostname from the host node, like host-0
            String virtualHostName = hostNode.getId();

            // Get physical hostname from the cluster like node3
            String physicalHostName = cluster.getHost(workerSlot.getNodeId());

            // Get corresponding physical port
            int physicalPort = workerSlot.getPort();

            List<ExecutorDetails> executorDetails = componentToExecutor.get(physicalOperatorName);

            // Reading out WorkerResources from graph node - these are needed in the assignment and are later
            // written into cgroups settings - see BasicContainer in storm-server
            WorkerResources resources = new WorkerResources();

            resources.put_to_resources(
                    Constants.HostProperties.BANDWIDTH,
                    hostNode.getAttribute(Constants.HostProperties.BANDWIDTH, Double.class));

            resources.put_to_resources(
                    Constants.HostProperties.CPU,
                    hostNode.getAttribute(Constants.HostProperties.CPU, Double.class));

            resources.put_to_resources(
                    Constants.HostProperties.RAM,
                    hostNode.getAttribute(Constants.HostProperties.RAM, Double.class));

            resources.put_to_resources(
                    Constants.HostProperties.RAMSWAP,
                    hostNode.getAttribute(Constants.HostProperties.RAMSWAP, Double.class));

            resources.put_to_resources(
                    Constants.HostProperties.LATENCY,
                    hostNode.getAttribute(Constants.HostProperties.LATENCY, Double.class));

            schedulerHosts.add(new SchedulerHost(
                    operatorNode.getId(),
                    operatorType,
                    virtualHostName,
                    physicalHostName,
                    physicalPort,
                    physicalOperatorName,
                    workerSlot,
                    resources,
                    executorDetails));
            hostNode.setAttribute("port", String.valueOf(physicalPort));
        }

        public ArrayList<SchedulerHost> getHosts() {
            return schedulerHosts;
        }

        public WorkerResources getWorkerResourcesBySlot(WorkerSlot slot){
            for (SchedulerHost host : schedulerHosts) {
                if (slot.equals(host.getWorkerSlot())) {
                    return host.getWorkerResources();
                }
            }
            return null;
        }
    }

    static class SchedulerHost {
        private final List<ExecutorDetails> executorDetails;

        private final WorkerSlot workerSlot;

        private final WorkerResources workerResources;

        public SchedulerHost(String operatorId,
                             String operatorType,
                             String virtualHostName,
                             String physicalHostName,
                             int physicalPort,
                             String physicalOperatorName,
                             WorkerSlot workerSlot,
                             WorkerResources resources,
                             List<ExecutorDetails> executorDetails) {
            this.executorDetails = executorDetails;
            this.workerSlot = workerSlot;
            this.workerResources = resources;

            LOG.info("Placing operator {} of type {} within component {} on {}/{}:{} with executors {} at slot {} and resources {}",
                    operatorId,
                    operatorType,           // e.g. sink, filter, spout..
                    physicalOperatorName,   // bolt2, spout3
                    virtualHostName,        // host-2, host-4,
                    physicalHostName,       // pc433.emulab.net
                    physicalPort,           // 6700
                    this.executorDetails.toString(),
                    this.workerSlot.getNodeId(),
                    resources);
        }
        public WorkerSlot getWorkerSlot() {
            return workerSlot;
        }

        public WorkerResources getWorkerResources() {
            return workerResources;
        }

        public List<ExecutorDetails> getExecutorDetails() {
            return executorDetails;
        }
    }
}

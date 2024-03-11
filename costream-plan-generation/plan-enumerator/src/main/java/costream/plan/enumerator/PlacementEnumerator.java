package costream.plan.enumerator;

import costream.plan.executor.main.Constants;
import org.graphstream.graph.Edge;
import org.graphstream.graph.EdgeRejectedException;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;

import costream.plan.executor.utils.GraphUtils;
import costream.plan.executor.utils.RanGen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static costream.plan.executor.utils.GraphUtils.*;
import static costream.plan.executor.utils.RanGen.randomBoolean;

public class PlacementEnumerator {
    private final int numOfHosts;
    private int groupingCounter;
    private int hostCounter;

    public PlacementEnumerator(Integer numOfHosts) {
        this.numOfHosts = numOfHosts;
        this.groupingCounter = 0;
        this.hostCounter = 0;
    }

    public void setHardwareConstraints(ArrayList<Graph> queryPlans) {
        for (Graph queryPlan : queryPlans) {
            String mode = (String) queryPlan.getAttribute(Constants.QueryProperties.MODE);
            if (mode.equals(Constants.QueryType.RANDOM)
                    || mode.startsWith("ad")
                    || mode.startsWith("smart-grid")
                    || mode.equals(Constants.QueryType.BENCHMARK.SPIKE_DETECTION)
                    || mode.contains("filter")) {
              addRandomConstraintsHierarchicalStrategy(queryPlan);
            } else {
                addFixedConstraints(queryPlan);
            }
        }
    }

    private static void addRandomConstraintsHierarchicalStrategy(Graph queryPlan){
        Node sink = GraphUtils.getSink(queryPlan);
        recursiveSetConstraints(sink, 3);
    }

    private static void recursiveSetConstraints(Node operatorGraphHead, int previousHWCategory) {
        if (operatorGraphHead.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SPOUT)) {
            return;
        }
        // Check if current host has already hardware constraints. If not, assign new ones and update previous score.
        Node hostGraphHead = getPlacementHost(operatorGraphHead);
        if (operatorGraphHead.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SINK)) {
            getRandomHardwareConfig(hostGraphHead, previousHWCategory);
        }

        for (Node preOperator : GraphUtils.getOperatorPredecessors(operatorGraphHead)) {
            Node preHost = getPlacementHost(preOperator);
            // if pre-host does not have a category yet, assign new hardware features
            int newHwCategory = previousHWCategory;

            if (!preHost.hasAttribute("category")) {
                if (randomBoolean()) {
                    // eventually reduce category by 1 but go not below 1
                    newHwCategory = Math.max(previousHWCategory-1 , 1);
                }
                getRandomHardwareConfig(preHost, newHwCategory);
            }
            recursiveSetConstraints(preOperator, newHwCategory);
        }
    }

    private static void getRandomHardwareConfig(Node node, int category) {
        assert (isOperator(node));
        int ram;
        int bandwidth;
        int latency;
        int cpu;

        switch (category) {
            case 1:
                ram = RanGen.randIntFromList(Constants.TrainingParams.SMALL_RAM_SIZES);
                cpu = RanGen.randIntFromList(Constants.TrainingParams.SMALL_CPU_SIZES);
                bandwidth = RanGen.randIntFromList(Constants.TrainingParams.SMALL_BANDWIDTHS);
                latency = RanGen.randIntFromList(Constants.TrainingParams.SMALL_LATENCIES);
                break;
            case 2:
                ram = RanGen.randIntFromList(Constants.TrainingParams.MEDIUM_RAM_SIZES);
                cpu = RanGen.randIntFromList(Constants.TrainingParams.MEDIUM_CPU_SIZES);
                bandwidth = RanGen.randIntFromList(Constants.TrainingParams.MEDIUM_BANDWIDTHS);
                latency = RanGen.randIntFromList(Constants.TrainingParams.MEDIUM_LATENCIES);
                break;
            case 3:
                ram = RanGen.randIntFromList(Constants.TrainingParams.LARGE_RAM_SIZES);
                cpu = RanGen.randIntFromList(Constants.TrainingParams.LARGE_CPU_SIZES);
                bandwidth = RanGen.randIntFromList(Constants.TrainingParams.LARGE_BANDWIDTHS);
                latency = RanGen.randIntFromList(Constants.TrainingParams.LARGE_LATENCIES);
                break;
            default:
                throw new RuntimeException("Category " + category + "should not exist!");
        }

        node.setAttribute(Constants.HostProperties.RAM, ram);
        node.setAttribute(Constants.HostProperties.RAMSWAP, ram + Constants.DEFAULT_SWAP_SIZE);
        node.setAttribute(Constants.HostProperties.CPU, cpu);
        node.setAttribute(Constants.HostProperties.LATENCY, latency);
        node.setAttribute(Constants.HostProperties.BANDWIDTH, bandwidth);
        node.setAttribute("category", category);
    }

    public static void addFixedConstraints(Graph queryPlan) {
        for (String prop : Arrays.asList(Constants.QueryProperties.COMMON_RAM,
                Constants.QueryProperties.COMMON_CPU,
                Constants.QueryProperties.COMMON_BANDWIDTH,
                Constants.QueryProperties.COMMON_LATENCY)) {
            if (!queryPlan.hasAttribute(prop)) {
                throw new RuntimeException("Query " + queryPlan.getId() + " has no common "+ prop + " specification");
            }
        }

        int commonRam = (int) queryPlan.getAttribute(Constants.QueryProperties.COMMON_RAM);
        int commonRamSwap = commonRam + Constants.DEFAULT_SWAP_SIZE;
        int commonCpu = (int) queryPlan.getAttribute(Constants.QueryProperties.COMMON_CPU);
        int commonBandwidth = (int) queryPlan.getAttribute(Constants.QueryProperties.COMMON_BANDWIDTH);
        int commonLatency = (int) queryPlan.getAttribute(Constants.QueryProperties.COMMON_LATENCY);

        for (Iterator<Node> it = queryPlan.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (!isOperator(node)) {
                node.setAttribute(Constants.HostProperties.RAM, commonRam);
                node.setAttribute(Constants.HostProperties.RAMSWAP, commonRamSwap);
                node.setAttribute(Constants.HostProperties.CPU, commonCpu);
                node.setAttribute(Constants.HostProperties.BANDWIDTH, commonBandwidth);
                node.setAttribute(Constants.HostProperties.LATENCY, commonLatency);;
            }
        }
    }

    public void addHardwareNodes(ArrayList<Graph> queryPlans) {
        for (Graph graph : queryPlans) {
            for (int i = 0; i < this.numOfHosts; i++) {
                Node host = graph.addNode("host-" + i);
                host.setAttribute(Constants.OperatorProperties.OPERATOR_TYPE, Constants.Operators.HOST);
            }
        }
    }

    /**
     * Adding a set of randomized placements to a list of given query plans.
     * The hosts have the name host-0, host-1, ...
     * Neighbored operators can be optionally co-located on the same host
     * This also considers grouping of operators here. This is related to Apache Storms Stream API and
     * its grouping strategy. See StreamBuilder.java for further information.
     *
     * @param queryPlans List of queryPlans that have not yet an assigned placement
     */
    public void addPlacements(ArrayList<Graph> queryPlans, boolean randomized, boolean ignoreEarlyFilters) {
        for (Graph graph : queryPlans) {
            groupingCounter = 0;
            hostCounter = 0;

            // if all operators on the same host, then place it directly and return here
            if (this.numOfHosts == 1) {
                // all on the same host
                for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
                    Node node = it.next();
                    if (isOperator(node)) {
                        graph.addEdge(RanGen.randString(10), node.getId(), graph.getNode("host-0").getId(), true);
                    }
                }
            } else {

                ArrayList<Node> earlyFilterCopy = new ArrayList<>();
                // Iterate over nodes, find groupings and place operators
                ArrayList<Node> groupingList = new ArrayList<>();
                if (!ignoreEarlyFilters) {
                    // This mimics storms behaviour of grouping early filters together - which is not very efficient!
                    ArrayList<Node> spouts = GraphUtils.getSpouts(graph);
                    ArrayList<Node> earlyFilters = new ArrayList<>();
                    for (Node spout : spouts) {
                        Node successor = spout.getLeavingEdge(0).getTargetNode();
                        if (successor.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.FILTER)) {
                            earlyFilters.add(successor);
                        }
                    }

                    earlyFilterCopy = new ArrayList<>(earlyFilters);
                    placeCurrentGroup(earlyFilters, graph, randomized);
                }

                // Iterate over nodes
                for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
                    Node node = it.next();
                    // only proceed for real operators
                    if (isOperator(node) && !earlyFilterCopy.contains(node)) {
                        switch ((String) node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE)) {
                            case Constants.Operators.SPOUT:
                            case Constants.Operators.SINK:
                                placeCurrentGroup(groupingList, graph, randomized);
                                groupingList.add(node);
                                placeCurrentGroup(groupingList, graph, randomized);
                                break;
                            case Constants.Operators.FILTER:
                            case Constants.Operators.AGGREGATE:
                                groupingList.add(node);
                                break;
                            case Constants.Operators.WINDOW_AGGREGATE:
                            case Constants.Operators.JOIN:
                                placeCurrentGroup(groupingList, graph, randomized);
                                groupingList.add(node);
                                break;
                            default:
                                throw new IllegalArgumentException(node.getAttribute("operatorType") + " has not been found yet");
                        }
                    }
                }

                // remove unassigned hosts from the graph
                ArrayList<String> sortedOut = new ArrayList<>();
                for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
                    Node node = it.next();
                    if (node != null && !isOperator(node) && !node.edges().iterator().hasNext()) {
                        sortedOut.add(node.getId());
                    }
                }
                for (String nodeToRemove : sortedOut) {
                    graph.removeNode(nodeToRemove);
                }
            }
        }
    }

    /**
     * This places a group of operators that belong together on the graph by adding corresponding host nodes
     * and edges. If the group has more than one node, we assign a "group" attribute.
     * This is later applied to highlight operator groupings.
     *
     * @param nodes List of nodes of a group
     * @param graph The current query graph
     */
    private void placeCurrentGroup(ArrayList<Node> nodes, Graph graph, boolean randomized) {
        if (nodes.isEmpty()) {
            return;
        }

        // add placement information
        if (nodes.size() > 1) {
            for (Node node : nodes) {
                if (!node.hasAttribute("grouping")) {
                    node.setAttribute("grouping", "group" + groupingCounter);
                }
            }
            groupingCounter++;
        }

        // check if group is already placed, then abort
        for (Node node : nodes) {
            for (Iterator<Edge> it = node.leavingEdges().iterator(); it.hasNext(); ) {
                Node target = it.next().getTargetNode();
                if (!isOperator(target)) {
                    nodes.clear();
                    return;
                }
            }
        }

        // else place group on a random host, allowing co-location on neighbors
        boolean placementFound = false;
        Node hostNode = null;

        while (!placementFound) {
            // get random node and verify
            if (randomized) {
                hostNode = graph.getNode("host-" + RanGen.randInt(0, this.numOfHosts - 1));
            } else {
                hostNode = graph.getNode("host-" + hostCounter++);
            }
            if (hostCounter >= numOfHosts) {
                hostCounter = 0;
            }
            // find valid placement for first node and co-locate the others
            placementFound = verifyPlacement(nodes.get(0), hostNode);
        }
        // getHostScore(hostNode);
        // add host-node to the graph
        for (Node node : nodes) {
            graph.addEdge(RanGen.randString(10), node.getId(), hostNode.getId(), true);
        }
        nodes.clear();
    }


    /**
     * This adds the network flow info to the graph. This means the way that the data is flowing through the
     * network / hosts. Due to the restrictions in the placement, there is no ping-pong allowed here.
     *
     * @param queryPlans List of plans that have a placement
     */
    public void addNetworkFlowInfo(ArrayList<Graph> queryPlans) {
        for (Graph graph : queryPlans) {
            for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
                Node node = it.next();
                if (isOperator(node)) {
                    Node host = null;           // host of current operator
                    Node successor = null;       // operator that follows the current one
                    Node successorHost = null;   // host of the successor
                    for (Iterator<Edge> it2 = node.leavingEdges().iterator(); it2.hasNext(); ) {
                        Edge edge = it2.next();
                        if (edge.getTargetNode().getId().startsWith("host")) {
                            host = edge.getTargetNode();
                        } else {
                            successor = edge.getTargetNode();
                        }
                    }
                    assert host != null;
                    if (successor != null && isOperator(successor)) {
                        if (isSink(node)) {
                            throw new RuntimeException("Sink of query " + graph.getId()+ " should not have a successor");
                        }
                        for (Iterator<Edge> it3 = successor.leavingEdges().iterator(); it3.hasNext(); ) {
                            Edge edge = it3.next();
                            if (edge.getTargetNode().getId().startsWith("host")) {
                                successorHost = edge.getTargetNode();
                            }
                        }
                        assert successorHost != null;
                        if (successorHost != host) {
                            try {
                                graph.addEdge(RanGen.randString(10), host, successorHost, true);
                            } catch (EdgeRejectedException ignored) {
                                // This can occur, if there is already an existing edge between both host nodes
                            }
                        }
                    }
                }
            }
        }
    }

    public static int getHostScore(Node host) {
        Integer cpu = host.getAttribute("cpu", Integer.class);
        Integer ram = host.getAttribute("ram", Integer.class);
        Integer bw = host.getAttribute("bandwidth", Integer.class);
        Integer lat = host.getAttribute("latency", Integer.class);
        int score = cpu + ram + bw + lat;
        System.out.println("host: " + host.getId() + " score: " + score);
        return score;
    }

    /**
     * Checks if a placement is valid. It is not valid, if nodes are placed on the host that are not direct neighbors
     * of the operator. Avoiding back-and-forth movements (ping-pong)
     *
     * @param operator Graph node of the operator to place
     * @param host     Host node to evaluate
     * @return boolean, if placement is valid
     */
    private static boolean verifyPlacement(Node operator, Node host) {
        // return true if no operators on host
        if (!host.edges().findAny().isPresent()) {
            return true;
        }
        // return true, if neighbors on the host to allow co-location of neighbored operators
        for (Iterator<Edge> it = host.edges().iterator(); it.hasNext(); ) {
            Edge edge = it.next();
            if (operator.hasEdgeFrom(edge.getSourceNode())) {
                return true;
            }
        }
        return false;
    }
}
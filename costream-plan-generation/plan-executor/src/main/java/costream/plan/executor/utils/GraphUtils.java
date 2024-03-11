package costream.plan.executor.utils;

import costream.plan.executor.main.Constants;
import org.apache.commons.lang.NotImplementedException;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;

import java.util.ArrayList;
import java.util.Iterator;

public class GraphUtils {
    public static boolean isSink(Node node) {
        return node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SINK);
    }

    public static boolean isSpout(Node node) {
        return node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.SPOUT);
    }

    public static boolean isFilter(Node node) {
        return node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.FILTER);
    }

    public static boolean isAggregation(Node node) {
        return node.getAttribute(Constants.OperatorProperties.OPERATOR_TYPE).equals(Constants.Operators.AGGREGATE);
    }
    public static boolean isOperator(Node node) {
        return !node.getAttribute("operatorType").equals(Constants.Operators.HOST);
    }

    public static int getNumOfHosts(Graph graph) {
        int numOfHosts = 0;
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (!isOperator(node)) {
                numOfHosts += 1;
            }
        }
        return numOfHosts;
    }

    public static ArrayList<Node> getSpouts(Graph graph) {
        ArrayList<Node> spouts = new ArrayList<>();
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (isOperator(node) && isSpout(node)) {
                spouts.add(node);
            }
        }
        return spouts;
    }

    public static Node getSink(Graph graph) {
        ArrayList<Node> sinks = new ArrayList<>();
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (isOperator(node) && isSink(node)) {
                sinks.add(node);
            }
        }
        if (sinks.size() != 1) {
            throw new NotImplementedException("Currently only single-sink queries are supported");
        }
        return sinks.get(0);
    }


    public static boolean hasOperatorSuccessor(Node node) {
        for (Iterator<Edge> it = node.leavingEdges().iterator(); it.hasNext(); ) {
            Edge nextEdge = it.next();
            Node successor = nextEdge.getTargetNode();
            if (isOperator(successor)) {
                return true;
            }
        }
        return false;
    }

    public static Node getOperatorSuccessor(Node node) {
        assert isOperator(node);
        ArrayList<Node> successors = new ArrayList<>();
        for (Iterator<Edge> it = node.leavingEdges().iterator(); it.hasNext(); ) {
            Edge nextEdge = it.next();
            Node successor = nextEdge.getTargetNode();
            if (isOperator(successor)) {
                successors.add(successor);
            }
        }
        assert successors.size() == 1;
        return successors.get(0);
    }

    public static ArrayList<Node> getOperatorPredecessors(Node node) {
        assert isOperator(node);
        ArrayList<Node> predecessors = new ArrayList<>();
        for (Iterator<Edge> it = node.enteringEdges().iterator(); it.hasNext(); ) {
            Edge edge = it.next();
            Node predecessor = edge.getSourceNode();
            if (isOperator(predecessor)) {
                predecessors.add(predecessor);
            }
        }
        return predecessors;
    }

    public static Node getPlacementHost(Node node) {
        assert isOperator(node);
        ArrayList<Node> hosts = new ArrayList<>();
        for (Iterator<Edge> it = node.leavingEdges().iterator(); it.hasNext(); ) {
            Edge edge = it.next();
            Node eventualHost = edge.getTargetNode();
            if (!isOperator(eventualHost)){
                hosts.add(eventualHost);
            }
        }
        assert hosts.size() == 1;
        return hosts.get(0);
    }

    public static Node getHostSuccessor(Node node) {
        assert isOperator(node);
        ArrayList<Node> successors = new ArrayList<>();
        for (Iterator<Edge> it = node.leavingEdges().iterator(); it.hasNext(); ) {
            Edge nextEdge = it.next();
            Node successor = nextEdge.getTargetNode();
            if (!isOperator(successor)) {
                successors.add(successor);
            }
        }
        if (successors.size() == 1) {
            return successors.get(0);
        } else if (successors.size() == 0) {
           return null;
        } else {
            throw new RuntimeException(node + "has more than one host successor");
        }
    }

    public static boolean isDAG(Graph graph) {
        for (Iterator<Node> it = graph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            if (isOperator(node) && node.leavingEdges().count() > 2) {
                return false;
            }
            if (!isOperator(node) && node.leavingEdges().count() > 1) {
                return false;
            }
        }
        return true;
    }

    public static ArrayList<Integer> getEventRates(Graph graph) {
        ArrayList<Integer> eventRates = new ArrayList<>();
        for (Node spout : getSpouts(graph)) {
            assert spout.hasAttribute("confEventRate");
            eventRates.add(((Double) spout.getAttribute("confEventRate")).intValue());
        }
        return eventRates;
    }

    public static ArrayList<Triple<Integer, Integer, Integer>> getTupleWidths(Graph graph) {
        ArrayList<Triple<Integer, Integer, Integer>> tupleWidths = new ArrayList<>();
        for (Node spout : getSpouts(graph)) {
            assert spout.hasAttribute("numInteger");
            assert spout.hasAttribute("numString");
            assert spout.hasAttribute("numDouble");
            tupleWidths.add(new Triple<>(((Double) spout.getAttribute("numInteger")).intValue(),
                    ((Double) spout.getAttribute("numDouble")).intValue(),
                    ((Double) spout.getAttribute("numString")).intValue()));
        }
        return tupleWidths;
    }
}

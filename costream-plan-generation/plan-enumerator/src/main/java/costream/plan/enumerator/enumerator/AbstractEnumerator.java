package costream.plan.enumerator.enumerator;

import costream.plan.enumerator.CombinationSpace;
import costream.plan.executor.application.synthetic.SyntheticSpoutOperator;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;
import costream.plan.executor.operators.OperatorProvider;
import costream.plan.executor.operators.WindowedJoinOperator;
import costream.plan.executor.operators.aggregation.AggregateOperator;
import costream.plan.executor.operators.filter.FilterOperator;
import costream.plan.executor.operators.map.MapPairOperator;
import costream.plan.executor.operators.window.WindowOperator;
import costream.plan.executor.utils.RanGen;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import costream.plan.executor.utils.Triple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

public abstract class AbstractEnumerator {
    protected final OperatorProvider operatorProvider;
    protected Graph currentGraph;                               // graph object holding all operators
    protected AbstractOperator<?> curGraphHead;                 // current head operator of the graph
    protected int currentOperatorIndex;                           // index of the current operator
    protected int currentEdgeIndex;                               // index of the current edge between two operators
    protected String currentQueryName;
    protected final ArrayList<Graph> planList;     // final list that holds the created
    protected final ArrayList<String> linearizedPlans;
    protected final List<List<Integer>> combinations;
    protected String mode;

    public AbstractEnumerator() {
        this.operatorProvider = new OperatorProvider();
        this.planList = new ArrayList<>();           // a list that is used for the storm topologies that are built
        this.linearizedPlans = new ArrayList<>();    // a list that holds all queries in a linear way to compare and check for possible duplicates
        this.combinations = new ArrayList<>();
    }

    public AbstractEnumerator(CombinationSpace space){
        this.operatorProvider = new OperatorProvider();
        this.planList = new ArrayList<>();          // a list that is used for the storm topologies that are built
        this.linearizedPlans = new ArrayList<>();   // a list that holds all queries in a linear way to compare and check for possible duplicates
        this.combinations = space.getCombinations();
    }

    public abstract void buildSingleQuery(int eventRate, int windowLength, int tupleWidth);

    public ArrayList<Graph> buildQueryPlans() {
        for (List<Integer> combination : combinations) {
            prepareNewQuery(new HashMap<String, Object>() {{
                put(Constants.QueryProperties.DURATION, combination.get(0));
                put(Constants.QueryProperties.EVENT_RATE, combination.get(1));
                put(Constants.QueryProperties.JVM_SIZE, combination.get(2));
                put(Constants.QueryProperties.COMMON_RAM, combination.get(3));
                put(Constants.QueryProperties.COMMON_CPU, combination.get(4));
                put(Constants.QueryProperties.COMMON_BANDWIDTH, combination.get(5));
                put(Constants.QueryProperties.WINDOW_LENGTH, combination.get(6));
                put(Constants.QueryProperties.TUPLE_WIDTH, combination.get(7));
                put(Constants.QueryProperties.COMMON_LATENCY, combination.get(8));
                put(Constants.QueryProperties.MODE, mode);
            }});
            buildSingleQuery(combination.get(1), combination.get(6), combination.get(7));
            finalizeQuery();
        }
        return planList;
    }

    protected void getLinearStreamWithFilter(int eventRate, int tupleWidth) {
        createFixedStream(new Triple<>(tupleWidth, tupleWidth, tupleWidth), eventRate);
        FilterOperator filterOperator = new FilterOperator((BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x > y, "greaterThan", Double.class);
        filterOperator.setLiteral(-10.0);
        filterOperator.setId(getOperatorIndex(true));
        addQueryVertex(filterOperator);
        addQueryEdge(curGraphHead, filterOperator);
        curGraphHead = filterOperator;
    }


    protected void prepareNewQuery(Map<String, Object> description) {
        currentQueryName = UUID.randomUUID().toString().replace("-", "").toUpperCase().substring(0,10);
        operatorProvider.reset();
        currentGraph = new SingleGraph(currentQueryName);
        curGraphHead = null;
        currentOperatorIndex = 0;
        currentEdgeIndex = 0;
        for (Map.Entry<String, Object> property : description.entrySet()) {
            currentGraph.setAttribute(property.getKey(), property.getValue());
        }
    }

    protected void finalizeQuery() {
        addSink();
        //if (noDuplicateFound(currentGraph)) {
        planList.add(currentGraph);
        //}
    }

    protected void createFixedStream(Triple<Integer, Integer, Integer> tupleWidth, int eventRate) {
        SyntheticSpoutOperator spout = (SyntheticSpoutOperator) operatorProvider.provideOperator(Constants.Operators.SPOUT, getOperatorIndex(true));
        spout.setTupleWidth(tupleWidth);
        spout.setEventRate(eventRate);
        addQueryVertex(spout);
        curGraphHead = spout;
    }

    protected void createRandomLinearStream() {
        String index = getOperatorIndex(true);
        SyntheticSpoutOperator spout =  new SyntheticSpoutOperator(RanGen.randIntFromList(Constants.TrainingParams.EVENT_RATES));
        spout.setId(index);
        addQueryVertex(spout);
        curGraphHead = spout;
    }

    protected void createRandomTwoWayJoinStream() {
        String index = getOperatorIndex(true);
        SyntheticSpoutOperator spout =  new SyntheticSpoutOperator(RanGen.randIntFromList(Constants.TrainingParams.TWO_WAY_JOIN_RATES));
        spout.setId(index);
        addQueryVertex(spout);
        curGraphHead = spout;
    }

    protected void createRandomThreeWayJoinStream() {
        String index = getOperatorIndex(true);
        SyntheticSpoutOperator spout =  new SyntheticSpoutOperator(RanGen.randIntFromList(Constants.TrainingParams.THREE_WAY_JOIN_RATES));
        spout.setId(index);
        addQueryVertex(spout);
        curGraphHead = spout;
    }

    protected void applyRandomOperator(String type) {
        AbstractOperator<?> operator = operatorProvider.provideOperator(type, getOperatorIndex(true));
        if (operator.equals(curGraphHead)) {
            throw new IllegalStateException("Operator "+ operator.getId() + " is the same as previous operator");
        }
        addQueryVertex(operator);
        addQueryEdge(curGraphHead, operator);
        curGraphHead = operator;
    }

    protected void applyWindowedAggregation(boolean groupByKey) {
        WindowOperator windowOperator = (WindowOperator) operatorProvider.provideOperator(Constants.Operators.WINDOW, null);
        if (!groupByKey) {
            AggregateOperator aggOperator = (AggregateOperator) operatorProvider.provideOperator(Constants.Operators.AGGREGATE, getOperatorIndex(true));
            aggOperator.addWindowDescription(windowOperator.getDescription());
            aggOperator.setGroupByClass("null");
            addQueryVertex(aggOperator);
            addQueryEdge(curGraphHead, aggOperator);
            curGraphHead = aggOperator;

        } else {
            MapPairOperator mapToPair = (MapPairOperator) operatorProvider.provideOperator(Constants.Operators.MAP_TO_PAIR, null);
            AggregateOperator aggOperator = (AggregateOperator) operatorProvider.provideOperator(Constants.Operators.PAIR_ITERABLE_AGGREGATE, getOperatorIndex(true));
            aggOperator.addWindowDescription(windowOperator.getDescription());
            aggOperator.setGroupByClass(mapToPair.getKlass().getSimpleName());
            addQueryVertex(aggOperator);
            addQueryEdge(curGraphHead, aggOperator);
            curGraphHead = aggOperator;
        }
    }

    protected void applyWindowedPairAggregation(boolean groupByKey) {
        if (!groupByKey) {
            AggregateOperator aggOperator = (AggregateOperator) operatorProvider.provideOperator(Constants.Operators.PAIR_AGGREGATE, getOperatorIndex(true));
            aggOperator.setGroupByClass("null");
            addQueryVertex(aggOperator);
            addQueryEdge(curGraphHead, aggOperator);
            curGraphHead = aggOperator;
        } else {
            MapPairOperator mapToPair = (MapPairOperator) operatorProvider.provideOperator(Constants.Operators.MAP_TO_PAIR, null);
            AggregateOperator aggOperator = (AggregateOperator) operatorProvider.provideOperator(Constants.Operators.PAIR_ITERABLE_AGGREGATE, getOperatorIndex(true));
            aggOperator.setGroupByClass(mapToPair.getKlass().getSimpleName());
            addQueryVertex(aggOperator);
            addQueryEdge(curGraphHead, aggOperator);
            curGraphHead = aggOperator;
        }
    }

    protected void joinTwoStreams(String h0, String h1) {
        MapPairOperator mapToPairOperator;
        mapToPairOperator = (MapPairOperator) operatorProvider.provideOperator(Constants.Operators.MAP_TO_PAIR, getOperatorIndex(false));
        WindowOperator windowOperator;
        windowOperator = (WindowOperator) operatorProvider.provideOperator(Constants.Operators.WINDOW, getOperatorIndex(false));

        // collect descriptions from map and window operator
        HashMap<String, Object> windowedJoinDescription = new HashMap<>();
        windowedJoinDescription.put(Constants.Features.joinKeyClass.name(), mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        windowedJoinDescription.putAll(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin = new WindowedJoinOperator(getOperatorIndex(true), windowedJoinDescription);

        // build graph
        addQueryVertex(windowedJoin);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(h0), currentGraph.getNode(windowedJoin.getId()), true);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(h1), currentGraph.getNode(windowedJoin.getId()), true);
        curGraphHead = windowedJoin;
    }

    protected void joinThreeStreams(String h0, String h1, String h2) {
        MapPairOperator mapToPairOperator;
        mapToPairOperator = (MapPairOperator) operatorProvider.provideOperator(Constants.Operators.MAP_TO_PAIR, getOperatorIndex(false));

        WindowOperator windowOperator;
        windowOperator = (WindowOperator) operatorProvider.provideOperator(Constants.Operators.WINDOW, getOperatorIndex(false));

        // collect descriptions from map and window operator
        HashMap<String, Object> windowedJoinDescription = new HashMap<>();
        windowedJoinDescription.put(Constants.Features.joinKeyClass.name(), mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        windowedJoinDescription.putAll(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin1 = new WindowedJoinOperator(getOperatorIndex(true), windowedJoinDescription);
        WindowedJoinOperator windowedJoin2 = new WindowedJoinOperator(getOperatorIndex(true), windowedJoinDescription);

        // build graph
        addQueryVertex(windowedJoin1);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(h0), currentGraph.getNode(windowedJoin1.getId()), true);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(h1), currentGraph.getNode(windowedJoin1.getId()), true);
        curGraphHead = windowedJoin1;

        addQueryVertex(windowedJoin2);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(windowedJoin1.getId()), currentGraph.getNode(windowedJoin2.getId()), true);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(h2), currentGraph.getNode(windowedJoin2.getId()), true);
        curGraphHead = windowedJoin2;
    }


    protected void addSink() {
        AbstractOperator<?> sink = new AbstractOperator<Object>() {
            @Override
            public HashMap<String, Object> getDescription() {
                HashMap<String, Object> description = super.getDescription();
                description.put("operatorType", Constants.Operators.SINK);
                return description;
            }
        };
        sink.setId(getOperatorIndex(true));
        addQueryVertex(sink);
        addQueryEdge(curGraphHead, sink);
        curGraphHead = sink;
    }

    protected boolean noDuplicateFound(Graph graph) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Node currentNode : graph) {
            HashMap<String, String> nodeAttributes = new HashMap<>();
            for (Object key : (currentNode.attributeKeys().toArray())) {
                String val = String.valueOf(currentNode.getAttribute((String) key));
                nodeAttributes.put((String) key, val);
            }
            nodeAttributes.remove("id");
            stringBuilder.append(nodeAttributes);
        }
        stringBuilder.append(graph.getAttribute(Constants.QueryProperties.DURATION));
        if (linearizedPlans.contains(stringBuilder.toString())) {
            System.out.print("Duplicate found");
            System.out.print(stringBuilder + "\n");
            return false;
        } else {
            linearizedPlans.add(stringBuilder.toString());
            return true;
        }
    }

    /**
     * Gives the current operator index with the query name and increases it
     *
     * @return operator index
     */
    protected String getOperatorIndex(boolean increment) {
        if (increment) {
            return (currentQueryName + "-" + currentOperatorIndex++);
        } else {
            return (currentQueryName + "-" + currentOperatorIndex);
        }
    }

    /**
     * Gives the current edge index with the query name and increases it
     *
     * @return edge index
     */
    protected String nextEdgeIndex() {
        return (currentQueryName + "-" + currentEdgeIndex++);
    }

    /**
     * This adds a query operator with its description to the graph object
     *
     * @param operator The operator to add
     */
    protected void addQueryVertex(AbstractOperator<?> operator) {
        Node n = currentGraph.addNode(operator.getId());
        n.setAttributes(operator.getDescription());
    }

    /**
     * This adds a query edge with its description to the graph object
     *
     * @param source The source operator
     * @param target The target operator
     */
    protected void addQueryEdge(AbstractOperator<?> source, AbstractOperator<?> target) {
        if (source.getId().equals(target.getId())) {
            throw new RuntimeException("Self-pointing loop for " + source.getId() + " and " + target.getId());
        }
        currentGraph.addEdge(
                nextEdgeIndex(),
                source.getId(),
                target.getId(),
                true);
    }

    public String getMode() {
        return mode;
    }
}


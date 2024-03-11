package costream.plan.executor.main;

import costream.plan.executor.operators.AbstractOperator;
import costream.plan.executor.operators.OperatorProvider;
import costream.plan.executor.operators.filter.FilterOperator;
import costream.plan.executor.operators.map.MapOperator;
import costream.plan.executor.operators.map.functions.AbstractMapFunction;
import costream.plan.executor.utils.EnrichedQueryGraph;
import costream.plan.executor.utils.RanGen;
import org.apache.storm.Config;
import org.apache.storm.streams.*;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;

import org.apache.storm.tuple.Tuple;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

/**
 * This abstract class has basic methods that are used in its implementation to automatically generate
 * storm topologies. The current operator graph is followed by a `graph`-Object that is later used in order to log
 * the query plan.
 */
abstract public class AbstractQueryBuilder {
    protected final OperatorProvider operatorProvider;
    protected Graph currentGraph;                     // graph object holding all operators
    protected AbstractOperator<?> curGraphHead;     // current head operator of the graph
    private int currentOperatorIndex;               // index of the current operator
    private int currentEdgeIndex;                   // index of the current edge between two operators
    protected String currentQueryName;
    protected StreamBuilder builder;
    protected final ArrayList<EnrichedQueryGraph> topoList;    // final list that holds the created
    protected final ArrayList<String> linearizedQueries;
    protected final String clusterName;                                        // current cluster name, is appended to log-files
    protected final Config config;

    @Deprecated
    public AbstractQueryBuilder(String host, Config config) {
        this.config = config;
        this.clusterName = host;
        this.operatorProvider = new OperatorProvider();
        this.topoList = new ArrayList<>(); // a list that is used for the storm topologies that are built
        this.linearizedQueries = new ArrayList<>(); // a list that holds all queries in a linear way to compare and check for possible duplicates
    }

    public abstract ArrayList<EnrichedQueryGraph> buildTopos(int numOfTopos);

    protected void prepareNewQuery(String queryName) {
        currentQueryName = clusterName + "-" + queryName;
        operatorProvider.reset();
        currentGraph = new SingleGraph(currentQueryName);

        builder = new StreamBuilder(config);
        curGraphHead = null;
        currentOperatorIndex = 0;
        currentEdgeIndex = 0;
    }

    protected Stream<DataTuple> newStream(AbstractOperator<?> spout, MapOperator map, String queryName) {
        Stream<Tuple> tupleStream = builder.newStream(spout.getSpoutOperator(queryName), 1, spout.getDescription());
        addQueryVertex(spout);
        curGraphHead = spout;
        return tupleStream.map((AbstractMapFunction<Tuple, DataTuple>) map.getFunction(), map.getDescription());
    }

    protected void finalizeTopo() {
            topoList.add(new EnrichedQueryGraph(builder.build(), currentGraph, currentQueryName));
    }

    /**
     * For some reason, the filter operator does not allow function calls in itself, since it becomes
     * unserializable then. So this has to be done separately.
     *
     * @param stream The Stream to filter
     * @param op     FilterOperator
     * @return Filtered stream
     */
    protected Stream<DataTuple> applyFilter(Stream<DataTuple> stream, FilterOperator op) {
        BiFunction<Object, Object, Boolean> func =
                (BiFunction<Object, Object, Boolean> & Serializable) op.getFunction();
        Class<?> klass = op.getKlass();
        Object literal = op.getLiteral();
        stream =
                stream.filter(
                        x -> func.apply(x.getTupleValue(klass, RanGen.randInt(0, x.getTupleContent().get(klass).size() - 1)), literal), op.getDescription());
        return stream;
    }

    /**
     * For some reason, the filter operator does not allow function calls in itself, since it becomes
     * unserializable then. So this has to be done separately.
     *
     * @param stream The Stream to filter
     * @param op     FilterOperator
     * @return Filtered stream
     */
    protected Stream<Pair<Object, DataTuple>> applyPairFilter(
            Stream<Pair<Object, DataTuple>> stream, FilterOperator op) {
        BiFunction<Object, Object, Boolean> func =
                (BiFunction<Object, Object, Boolean> & Serializable) op.getFunction();
        Class<?> klass = op.getKlass();
        Object literal = op.getLiteral();
        stream =
                stream.filter(
                        x ->
                                func.apply(
                                        x.getSecond()
                                                .getTupleValue(klass, RanGen.randInt(0, x.getSecond().getTupleContent().get(klass).size() - 1)),
                                        literal), op.getDescription());
        return stream;
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
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(source.getId()),
                currentGraph.getNode(target.getId()));
    }
}

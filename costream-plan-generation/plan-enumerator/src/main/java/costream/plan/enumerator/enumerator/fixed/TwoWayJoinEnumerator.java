package costream.plan.enumerator.enumerator.fixed;

import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.enumerator.CombinationSpace;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.WindowedJoinOperator;
import costream.plan.executor.operators.map.MapPairOperator;
import costream.plan.executor.operators.window.WindowOperator;
import costream.plan.executor.utils.Triple;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.HashMap;


public class TwoWayJoinEnumerator extends AbstractEnumerator {

    public TwoWayJoinEnumerator(CombinationSpace space) {
        super(space);
        this.mode = Constants.QueryType.FIXED.TWO_WAY_JOIN;
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        getLinearStreamWithFilter(eventRate, tupleWidth);
        String head0 = curGraphHead.getId();

        // second stream
        createFixedStream(new Triple<>(tupleWidth, tupleWidth, tupleWidth), eventRate); // todo: Add second event rate here
        String head1 = curGraphHead.getId();

        // map
        MapPairOperator mapToPairOperator = new MapPairOperator(
                input -> Pair.of(input.getTupleValue(Integer.class, 0), input), Integer.class);

        // window
        BaseWindowedBolt.Duration window = BaseWindowedBolt.Duration.of(windowLength * 1000);
        WindowOperator windowOperator = new WindowOperator("tumblingWindow", "duration", TumblingWindows.of(window), window.value, null);

        // join
        HashMap<String, Object> windowedJoinDescription = new HashMap<>();
        windowedJoinDescription.put(Constants.Features.joinKeyClass.name(), mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        windowedJoinDescription.putAll(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin = new WindowedJoinOperator(getOperatorIndex(true), windowedJoinDescription);

        // build graph
        addQueryVertex(windowedJoin);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(head0), currentGraph.getNode(windowedJoin.getId()), true);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(head1), currentGraph.getNode(windowedJoin.getId()), true);
        curGraphHead = windowedJoin;
    }
}

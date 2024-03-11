package costream.plan.enumerator.enumerator.fixed;

import costream.plan.enumerator.CombinationSpace;
import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.aggregation.AggregateOperator;
import costream.plan.executor.operators.aggregation.functions.AggregateSumFunction;
import costream.plan.executor.operators.window.WindowOperator;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class LinearAggCountEnumerator extends AbstractEnumerator {

    public LinearAggCountEnumerator(CombinationSpace space) {
        super(space);
        this.mode = Constants.QueryType.FIXED.LINEAR_AGG_COUNT;
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        getLinearStreamWithFilter(eventRate, tupleWidth);

        // fixed windowed aggregation
        BaseWindowedBolt.Count window = BaseWindowedBolt.Count.of(1000);
        WindowOperator windowOperator = new WindowOperator("tumblingWindow", "count", TumblingWindows.of(window), window.value, null);
        AggregateOperator aggregateOperator = new AggregateOperator(new AggregateSumFunction<>(Integer.class), "sum", Integer.class);
        aggregateOperator.addWindowDescription(windowOperator.getDescription());
        aggregateOperator.setGroupByClass("null");
        aggregateOperator.setId(getOperatorIndex(true));
        addQueryVertex(aggregateOperator);
        addQueryEdge(curGraphHead, aggregateOperator);
        curGraphHead = aggregateOperator;
    }
}

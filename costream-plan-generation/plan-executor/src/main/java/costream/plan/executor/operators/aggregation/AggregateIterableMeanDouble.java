package costream.plan.executor.operators.aggregation;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.operators.aggregation.functions.AggregateMeanFunction;
import org.apache.storm.streams.Pair;

import java.util.ArrayList;

public class AggregateIterableMeanDouble extends AbstractAggregateIterableFunction {
    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return Pair.of(accum.getFirst(), new AggregateMeanFunction<>(Double.class).result(accum.getSecond()));
    }
}

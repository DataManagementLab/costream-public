package costream.plan.executor.operators.aggregation;

import costream.plan.executor.operators.aggregation.functions.AggregateMinFunction;
import costream.plan.executor.main.DataTuple;
import org.apache.storm.streams.Pair;

import java.util.ArrayList;

public class AggregatePairMinDouble extends AbstractAggregatePairFunction {
    @Override
    public Pair<Object, DataTuple> result(ArrayList<Pair<Object, DataTuple>> accum) {
        return Pair.of(null, new AggregateMinFunction<>(Double.class).result(tupleToList(accum)));
    }
}

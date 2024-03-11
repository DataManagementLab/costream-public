package costream.plan.executor.operators.aggregation;

import costream.plan.executor.main.DataTuple;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.CombinerAggregator;

import java.util.ArrayList;

public class AbstractAggregateIterableFunction implements
        CombinerAggregator<Pair<Object, Iterable<DataTuple>>, Pair<Object, ArrayList<DataTuple>>, Pair<Object, DataTuple>> {

    @Override
    public Pair<Object, ArrayList<DataTuple>> init() {
        return Pair.of(null, new ArrayList<>());
    }

    @Override
    public Pair<Object, ArrayList<DataTuple>> apply(Pair<Object, ArrayList<DataTuple>> accumulator, Pair<Object, Iterable<DataTuple>> value) {
        Object tupleKey = value.getFirst();
        ArrayList<DataTuple> tupleList = accumulator.getSecond();
        for (DataTuple tuple : value.getSecond()) {
            tupleList.add(tuple);
        }
        return Pair.of(tupleKey, tupleList);
    }

    @Override
    public Pair<Object, ArrayList<DataTuple>> merge(Pair<Object, ArrayList<DataTuple>> accum1, Pair<Object, ArrayList<DataTuple>> accum2) {
        assert accum1.getFirst().equals(accum2.getFirst());
        ArrayList<DataTuple> accum = accum1.getSecond();
        accum.addAll(accum2.getSecond());
        return Pair.of(accum1.getFirst(), accum);
    }

    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return null;
    }
}

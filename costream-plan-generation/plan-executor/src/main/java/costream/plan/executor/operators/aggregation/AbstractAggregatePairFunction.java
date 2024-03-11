package costream.plan.executor.operators.aggregation;

import costream.plan.executor.main.DataTuple;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.CombinerAggregator;

import java.util.ArrayList;

public abstract class AbstractAggregatePairFunction implements
        CombinerAggregator<Pair<Object, DataTuple>, ArrayList<Pair<Object, DataTuple>>, Pair<Object, DataTuple>> {
    @Override
    public ArrayList<Pair<Object, DataTuple>> init() {
        return new ArrayList<>();
    }

    @Override
    public ArrayList<Pair<Object, DataTuple>> apply(ArrayList<Pair<Object, DataTuple>> accumulator, Pair<Object, DataTuple> value) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public ArrayList<Pair<Object, DataTuple>> merge(ArrayList<Pair<Object, DataTuple>> accum1, ArrayList<Pair<Object, DataTuple>> accum2) {
        accum1.addAll(accum2);
        return accum1;
    }

    protected ArrayList<DataTuple> tupleToList(ArrayList<Pair<Object, DataTuple>> accum) {
        ArrayList<DataTuple> accumData = new ArrayList<>();
        for (Pair<Object, DataTuple> pair : accum) {
            accumData.add(pair.getSecond());
        }
        return accumData;
    }
}

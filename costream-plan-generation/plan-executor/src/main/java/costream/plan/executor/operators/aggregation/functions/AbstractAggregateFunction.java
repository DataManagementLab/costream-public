package costream.plan.executor.operators.aggregation.functions;

import costream.plan.executor.main.DataTuple;
import org.apache.storm.streams.operations.CombinerAggregator;

import java.util.ArrayList;


abstract public class AbstractAggregateFunction implements CombinerAggregator<DataTuple, ArrayList<DataTuple>, DataTuple> {
    public final Class<?> klass;
    public AbstractAggregateFunction(Class<?> klass) {
        this.klass = klass;
    }

    @Override
    public ArrayList<DataTuple> init() {
        return new ArrayList<>();
    }

    @Override
    public ArrayList<DataTuple> apply(ArrayList<DataTuple> accumulator, DataTuple value) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public ArrayList<DataTuple> merge(ArrayList<DataTuple> accum1, ArrayList<DataTuple> accum2) {
        accum1.addAll(accum2);
        return accum1;
    }

    /**
     * Initializes an array list with specified values, e.g. an array of [0.0, 0.0...]
     * @param len Length of array
     * @param val Value to insert
     * @return array list
     */
    public ArrayList<Object> getInitList(int len, int val) {
        ArrayList<Object> initList = new ArrayList<>();
        if (klass == Integer.class) {
            for (int i = 0; i < len; i++) {
                initList.add(klass.cast(val));
            }
        } else if (klass == Double.class) {
            for (int i = 0; i < len; i++) {
                initList.add((double) val);
            }
        }
        return initList;
    }

    public Long findE2ETimestamp(ArrayList<DataTuple> tupleList){
        long timestamp = tupleList.get(0).getE2ETimestamp();
        for (DataTuple tuple : tupleList){
            if (tuple.getE2ETimestamp() > timestamp){
                timestamp = tuple.getE2ETimestamp();
            }
        }
        return timestamp;
    }

    public Long findProcessingTimestamp(ArrayList<DataTuple> tupleList){
        long timestamp = tupleList.get(0).getProcessingTimestamp();
        for (DataTuple tuple : tupleList){
            if (tuple.getProcessingTimestamp() > timestamp){
                timestamp = tuple.getProcessingTimestamp();
            }
        }
        return timestamp;
    }
}

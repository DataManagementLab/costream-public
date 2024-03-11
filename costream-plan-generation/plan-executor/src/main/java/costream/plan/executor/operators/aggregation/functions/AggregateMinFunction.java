package costream.plan.executor.operators.aggregation.functions;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;

import java.util.ArrayList;

public class AggregateMinFunction<S extends Number> extends AbstractAggregateFunction {

    public AggregateMinFunction(Class<S> klass) {
        super(klass);
    }

    @Override
    public DataTuple result(ArrayList<DataTuple> accum) {
        DataTuple firstTuple = accum.get(0);
        String queryName = firstTuple.getQueryName();
        TupleContent tupleContent = new TupleContent(firstTuple.getNumValuesPerDataType(), true);
        int numOfValuesInClass = firstTuple.getTupleContent().get(klass).size();
        ArrayList<Object> lowestValues = this.getInitList(numOfValuesInClass, 100000);

        for (DataTuple tuple : accum) {
            ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
            for (int i = 0; i < classContent.size(); i++) {
                Object result = lowestValues.get(i);
                if (klass == Integer.class) {
                    if ((int) classContent.get(i) < (int) lowestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else if (klass == Double.class) {
                    if ((double) classContent.get(i) < (double) lowestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else {
                    throw new RuntimeException("Class is not defined");
                }
                lowestValues.set(i, result);
            }
            tupleContent.put(klass, lowestValues);
        }
        // use just oldest timestamp
        return new DataTuple(tupleContent, findE2ETimestamp(accum), findProcessingTimestamp(accum), queryName);
    }
}

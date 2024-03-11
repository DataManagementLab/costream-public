package costream.plan.executor.operators.aggregation.functions;

import costream.plan.executor.main.Constants;
import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;

import java.util.ArrayList;

public class AggregateMaxFunction<S extends Number> extends AbstractAggregateFunction {
    /**
     * Gives the maximum value(s) that is applied on a given window.
     *
     * @param klass Class type of which values to look for maximum
     */
    public AggregateMaxFunction(Class<S> klass) {
        super(klass);
    }

    @Override
    public DataTuple result(ArrayList<DataTuple> accum) {
        DataTuple firstTuple = accum.get(0);
        String queryName = firstTuple.getQueryName();
        TupleContent tupleContent = new TupleContent(firstTuple.getNumValuesPerDataType(), true);
        int numOfValuesInClass = firstTuple.getTupleContent().get(klass).size();

        ArrayList<Object> highestValues = this.getInitList(numOfValuesInClass, Constants.TrainingParams.INTEGER_VALUE_RANGE.value1);

        for (DataTuple tuple : accum) {
            ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
            for (int i = 0; i < classContent.size(); i++) {
                Object result = highestValues.get(i);
                if (klass == Integer.class) {
                    if ((int) classContent.get(i) > (int) highestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else if (klass == Double.class) {
                    if ((double) classContent.get(i) > (double) highestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else {
                    throw new RuntimeException("Class is not defined");
                }
                highestValues.set(i, result);
            }
            tupleContent.put(klass, highestValues);

        }
        // use just oldest timestamp
        return new DataTuple(tupleContent, findE2ETimestamp(accum), findProcessingTimestamp(accum), queryName);
    }
}
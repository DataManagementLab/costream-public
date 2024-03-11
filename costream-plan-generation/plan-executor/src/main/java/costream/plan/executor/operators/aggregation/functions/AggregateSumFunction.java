package costream.plan.executor.operators.aggregation.functions;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;

import java.util.ArrayList;

public class AggregateSumFunction<T> extends AbstractAggregateFunction {
  /**
   * Sum for each tuple field of integers or doubles along a window
   *
   * @param klass Class of values to sum up
   */
  public AggregateSumFunction(Class<T> klass) {
    super(klass);
  }

  @Override
  public DataTuple result(ArrayList<DataTuple> accum) {
    DataTuple firstTuple = accum.get(0);
    String queryName = firstTuple.getQueryName();
    TupleContent tupleContent = new TupleContent(firstTuple.getNumValuesPerDataType(), true);
    int numOfValuesInClass = firstTuple.getTupleContent().get(klass).size();
    ArrayList<Object> sums = this.getInitList(numOfValuesInClass, 0);

    for (DataTuple tuple : accum) {
      ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
      for (int i = 0; i < classContent.size(); i++) {
        Object result = null;
        if (klass == Integer.class) {
          result = (int) sums.get(i) + (int) tuple.getTupleValue(klass, i);
        }
        if (klass == Double.class) {
          result = (double) sums.get(i) + (double) tuple.getTupleValue(klass, i);
        }
        sums.set(i, result);
      }
    }
    tupleContent.put(klass, sums);
    return new DataTuple(tupleContent, findE2ETimestamp(accum), findProcessingTimestamp(accum), queryName);
  }
}

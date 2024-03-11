package costream.plan.executor.operators.aggregation.functions;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;

import java.util.ArrayList;

public class AggregateMeanFunction<S extends Number> extends AbstractAggregateFunction {
  /**
   * Gives the mean value(s) that is applied on a given window.
   *
   * @param klass Class type of which values to look for mean
   */
  public AggregateMeanFunction(Class<S> klass) {
    super(klass);
  }

  @Override
  public DataTuple result(ArrayList<DataTuple> accum) {
    DataTuple firstTuple = accum.get(0);
    String queryName = firstTuple.getQueryName();
    TupleContent tupleContent = new TupleContent(firstTuple.getNumValuesPerDataType(), true);
    int numOfValuesInClass = firstTuple.getTupleContent().get(klass).size();

    ArrayList<Object> means = this.getInitList(numOfValuesInClass, 0);

    for (DataTuple tuple : accum) {
      ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
      for (int i = 0; i < classContent.size(); i++) {
        Object result = null;
        if (klass == Integer.class) {
          result = (int) means.get(i) + (int) tuple.getTupleValue(klass, i);
        }
        if (klass == Double.class) {
          result = (double) means.get(i) + (double) tuple.getTupleValue(klass, i);
        }
        means.set(i, result);
      }
    }

    // get means
    for (int i = 0; i < means.size(); i++) {
      Object result = null;
      if (klass == Integer.class) {
        result = (int) means.get(i) / accum.size();
      }
      if (klass == Double.class) {
        result = (double) means.get(i) / accum.size();
      }
      means.set(i, result);
    }
    tupleContent.put(klass, means);
    return new DataTuple(tupleContent, findE2ETimestamp(accum), findProcessingTimestamp(accum), queryName);
  }
}

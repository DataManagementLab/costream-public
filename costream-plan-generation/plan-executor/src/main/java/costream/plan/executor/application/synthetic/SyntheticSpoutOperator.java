package costream.plan.executor.application.synthetic;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;
import costream.plan.executor.utils.RanGen;
import costream.plan.executor.utils.Triple;

import java.util.*;

/**
 * This is the basic spout operator that holds a data source with a configurable event rate
 */
public class SyntheticSpoutOperator extends AbstractOperator<Object> {
    private Triple<Integer, Integer, Integer> numTupleDatatypes; // int, double, string
    private double eventRate;


  public SyntheticSpoutOperator(double throughput) {
    super();
    this.eventRate = throughput;
    this.numTupleDatatypes = new Triple<>(
            RanGen.randIntRange(Constants.TrainingParams.INTEGER_RANGE),
            RanGen.randIntRange(Constants.TrainingParams.DOUBLE_RANGE),
            RanGen.randIntRange(Constants.TrainingParams.STRING_RANGE));
  }


  @Override
  public HashMap<String, Object> getDescription() {
    HashMap<String, Object> description = super.getDescription();
    description.put("operatorType", Constants.Operators.SPOUT);
    description.put("confEventRate", (int) eventRate);
    description.put("numInteger", numTupleDatatypes.getFirst());
    description.put("numDouble", numTupleDatatypes.getSecond());
    description.put("numString", numTupleDatatypes.getThird());
    description.put("tupleWidthOut", numTupleDatatypes.getSum() + 2); // add timestamp value manually
    return description;
  }

  public void setEventRate(double eventRate){
    this.eventRate = eventRate;
  }

  public void setTupleWidth(Triple<Integer, Integer, Integer> tupleWidthPerDatatype){
    this.numTupleDatatypes = tupleWidthPerDatatype;
  }
}

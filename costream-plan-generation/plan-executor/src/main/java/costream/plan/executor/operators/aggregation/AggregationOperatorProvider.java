package costream.plan.executor.operators.aggregation;

import costream.plan.executor.operators.aggregation.functions.AbstractAggregateFunction;
import costream.plan.executor.operators.aggregation.functions.AggregateMaxFunction;
import costream.plan.executor.operators.aggregation.functions.AggregateMinFunction;
import costream.plan.executor.operators.AbstractOperatorProvider;
import costream.plan.executor.operators.aggregation.functions.AggregateMeanFunction;
import costream.plan.executor.operators.aggregation.functions.AggregateSumFunction;

import java.util.Arrays;

/**
 * This class provides Aggregation operators that can be applied on a storm topology
 */
public class AggregationOperatorProvider extends AbstractOperatorProvider<AggregateOperator> {
    public AggregationOperatorProvider() {
        this.supportedClasses.addAll(Arrays.asList(Integer.class, Double.class));
        operators.add(new AggregateOperator(new AggregateSumFunction<>(Integer.class), "sum", Integer.class));
        operators.add(new AggregateOperator(new AggregateSumFunction<>(Double.class), "sum", Double.class));
        operators.add(new AggregateOperator(new AggregateMeanFunction<>(Double.class), "mean", Double.class));
        operators.add(new AggregateOperator(new AggregateMeanFunction<>(Integer.class), "mean", Integer.class));
        operators.add(new AggregateOperator(new AggregateMaxFunction<>(Integer.class), "max", Integer.class));
        operators.add(new AggregateOperator(new AggregateMaxFunction<>(Double.class), "max", Double.class));
        operators.add(new AggregateOperator(new AggregateMinFunction<>(Integer.class), "min", Integer.class));
        operators.add(new AggregateOperator(new AggregateMinFunction<>(Double.class), "min", Double.class));
    }

    public AbstractAggregateFunction getAggregator(String aggFunction, Class<?> aggClass) {
        if (aggFunction.equals("sum") && aggClass == Integer.class) {
            return new AggregateSumFunction<>(Integer.class);
        } else if (aggFunction.equals("sum") && aggClass == Double.class) {
            return new AggregateSumFunction<>(Double.class);
        } else if (aggFunction.equals("mean") && aggClass == Integer.class) {
            return new AggregateMeanFunction<>(Integer.class);
        } else if (aggFunction.equals("mean") && aggClass == Double.class) {
            return new AggregateMeanFunction<>(Double.class);
        } else if (aggFunction.equals("max") && aggClass == Integer.class) {
            return new AggregateMaxFunction<>(Integer.class);
        } else if (aggFunction.equals("max") && aggClass == Double.class) {
            return new AggregateMaxFunction<>(Double.class);
        } else if (aggFunction.equals("min") && aggClass == Integer.class) {
            return new AggregateMinFunction<>(Integer.class);
        } else if (aggFunction.equals("min") && aggClass == Double.class) {
            return new AggregateMinFunction<>(Double.class);
        }
        throw new IllegalArgumentException("No aggregator found for " + aggFunction + " " + aggClass.toString());
    }
}
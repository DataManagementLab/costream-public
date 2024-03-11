package costream.plan.executor.operators.aggregation;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.operators.aggregation.functions.AggregateMaxFunction;
import costream.plan.executor.operators.aggregation.functions.AggregateMinFunction;
import costream.plan.executor.operators.aggregation.functions.AggregateSumFunction;
import costream.plan.executor.operators.AbstractOperatorProvider;
import org.apache.storm.streams.Pair;

import java.util.ArrayList;

public class AggregationIterableOperatorProvider extends AbstractOperatorProvider<AggregateOperator> {
    public AggregationIterableOperatorProvider() {
        this.supportedClasses.add(Integer.class);
        this.supportedClasses.add(Double.class);
        operators.add(new AggregateOperator(new AggregateIterableMinInteger(), "min", Integer.class));
        operators.add(new AggregateOperator(new AggregateIterableMinDouble(), "min", Double.class));
        operators.add(new AggregateOperator(new AggregateIterableMaxInteger(), "max", Integer.class));
        operators.add(new AggregateOperator(new AggregateIterableMaxDouble(), "max", Double.class));
        operators.add(new AggregateOperator(new AggregateIterableMeanInteger(), "mean", Integer.class));
        operators.add(new AggregateOperator(new AggregateIterableMeanDouble(), "mean", Double.class));
        operators.add(new AggregateOperator(new AggregateIterableSumInteger(), "sum", Integer.class));
        operators.add(new AggregateOperator(new AggregateIterableSumDouble(), "sum", Double.class));
    }

    public AbstractAggregateIterableFunction getIterableAggregator(String aggFunction, Class<?> aggClass) {
        if (aggFunction.equals("sum") && aggClass == Integer.class) {
            return new AggregateIterableSumInteger();
        } else if (aggFunction.equals("sum") && aggClass == Double.class) {
            return new AggregateIterableSumDouble();
        } else if (aggFunction.equals("mean") && aggClass == Integer.class) {
            return new AggregateIterableMeanInteger();
        } else if (aggFunction.equals("mean") && aggClass == Double.class) {
            return new AggregateIterableMeanDouble();
        } else if (aggFunction.equals("max") && aggClass == Integer.class) {
            return new AggregateIterableMaxInteger();
        } else if (aggFunction.equals("max") && aggClass == Double.class) {
            return new AggregateIterableMaxDouble();
        } else if (aggFunction.equals("min") && aggClass == Integer.class) {
            return new AggregateIterableMinInteger();
        } else if (aggFunction.equals("min") && aggClass == Double.class) {
            return new AggregateIterableMinDouble();
        }
        throw new IllegalArgumentException("No aggregator found for " + aggFunction + " " + aggClass.toString());
    }
}

class AggregateIterableMinInteger extends AbstractAggregateIterableFunction {
    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return Pair.of(accum.getFirst(), new AggregateMinFunction<>(Integer.class).result(accum.getSecond()));
    }
}

class AggregateIterableMinDouble extends AbstractAggregateIterableFunction {
    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return Pair.of(accum.getFirst(), new AggregateMinFunction<>(Double.class).result(accum.getSecond()));
    }
}


class AggregateIterableMaxInteger extends AbstractAggregateIterableFunction {
    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return Pair.of(accum.getFirst(), new AggregateMaxFunction<>(Integer.class).result(accum.getSecond()));
    }
}


class AggregateIterableMaxDouble extends AbstractAggregateIterableFunction {
    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return Pair.of(accum.getFirst(), new AggregateMaxFunction<>(Double.class).result(accum.getSecond()));
    }
}


class AggregateIterableSumInteger extends AbstractAggregateIterableFunction {
    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return Pair.of(accum.getFirst(), new AggregateSumFunction<>(Integer.class).result(accum.getSecond()));
    }
}


class AggregateIterableSumDouble extends AbstractAggregateIterableFunction {
    @Override
    public Pair<Object, DataTuple> result(Pair<Object, ArrayList<DataTuple>> accum) {
        return Pair.of(accum.getFirst(), new AggregateSumFunction<>(Double.class).result(accum.getSecond()));
    }
}

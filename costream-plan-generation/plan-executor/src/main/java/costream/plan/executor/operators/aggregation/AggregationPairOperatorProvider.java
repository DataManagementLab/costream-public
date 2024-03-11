package costream.plan.executor.operators.aggregation;

import costream.plan.executor.operators.aggregation.functions.AggregateMaxFunction;
import costream.plan.executor.main.DataTuple;
import costream.plan.executor.operators.AbstractOperatorProvider;
import costream.plan.executor.operators.aggregation.functions.AggregateMeanFunction;
import costream.plan.executor.operators.aggregation.functions.AggregateSumFunction;
import org.apache.storm.streams.Pair;

import java.util.ArrayList;

public class AggregationPairOperatorProvider extends AbstractOperatorProvider<AggregateOperator> {
    public AggregationPairOperatorProvider() {
        this.supportedClasses.add(Integer.class);
        this.supportedClasses.add(Double.class);
        operators.add(new AggregateOperator(new AggregatePairMinInteger(), "min", Integer.class));
        operators.add(new AggregateOperator(new AggregatePairMinDouble(), "min", Double.class));
        operators.add(new AggregateOperator(new AggregatePairMaxInteger(), "max", Integer.class));
        operators.add(new AggregateOperator(new AggregatePairMaxDouble(), "max", Double.class));
        operators.add(new AggregateOperator(new AggregatePairMeanInteger(), "mean", Integer.class));
        operators.add(new AggregateOperator(new AggregatePairMeanDouble(), "mean", Double.class));
        operators.add(new AggregateOperator(new AggregatePairSumInteger(), "sum", Integer.class));
        operators.add(new AggregateOperator(new AggregatePairSumDouble(), "sum", Double.class));
    }


    public AbstractAggregatePairFunction getAggregator(String aggFunction, Class<?> aggClass) {
        if (aggFunction.equals("sum") && aggClass == Integer.class) {
            return new AggregatePairSumInteger();
        } else if (aggFunction.equals("sum") && aggClass == Double.class) {
            return new AggregatePairSumDouble();
        } else if (aggFunction.equals("mean") && aggClass == Integer.class) {
            return new AggregatePairMeanInteger();
        } else if (aggFunction.equals("mean") && aggClass == Double.class) {
            return new AggregatePairMeanDouble();
        } else if (aggFunction.equals("max") && aggClass == Integer.class) {
            return new AggregatePairMaxInteger();
        } else if (aggFunction.equals("max") && aggClass == Double.class) {
            return new AggregatePairMaxDouble();
        } else if (aggFunction.equals("min") && aggClass == Integer.class) {
            return new AggregatePairMinInteger();
        } else if (aggFunction.equals("min") && aggClass == Double.class) {
            return new AggregatePairMinDouble();
        }
        throw new IllegalArgumentException("No aggregator found for " + aggFunction + " " + aggClass.toString());
    }
}


class AggregatePairMaxInteger extends AbstractAggregatePairFunction {
    @Override
    public Pair<Object, DataTuple> result(ArrayList<Pair<Object, DataTuple>> accum) {
        return Pair.of(null, new AggregateMaxFunction<>(Integer.class).result(tupleToList(accum)));
    }
}

class AggregatePairMaxDouble extends AbstractAggregatePairFunction {
    @Override
    public Pair<Object, DataTuple> result(ArrayList<Pair<Object, DataTuple>> accum) {
        return Pair.of(null, new AggregateMaxFunction<>(Double.class).result(tupleToList(accum)));
    }
}

class AggregatePairMeanInteger extends AbstractAggregatePairFunction {
    @Override
    public Pair<Object, DataTuple> result(ArrayList<Pair<Object, DataTuple>> accum) {
        return Pair.of(null, new AggregateMeanFunction<>(Integer.class).result(tupleToList(accum)));
    }
}

class AggregatePairMeanDouble extends AbstractAggregatePairFunction {
    @Override
    public Pair<Object, DataTuple> result(ArrayList<Pair<Object, DataTuple>> accum) {
        return Pair.of(null, new AggregateMeanFunction<>(Double.class).result(tupleToList(accum)));
    }
}

class AggregatePairSumInteger extends AbstractAggregatePairFunction {
    @Override
    public Pair<Object, DataTuple> result(ArrayList<Pair<Object, DataTuple>> accum) {
        return Pair.of(null, new AggregateSumFunction<>(Integer.class).result(tupleToList(accum)));
    }
}

class AggregatePairSumDouble extends AbstractAggregatePairFunction {
    @Override
    public Pair<Object,DataTuple> result(ArrayList<Pair<Object, DataTuple>> accum) {
        return Pair.of(null, new AggregateSumFunction<>(Double.class).result(tupleToList(accum)));
    }
}


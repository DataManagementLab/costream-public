package costream.plan.executor.operators.map;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.operators.AbstractOperatorProvider;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.PairFunction;

// todo: Choose an appropriate index
public class MapPairOperatorProvider extends AbstractOperatorProvider<MapPairOperator> {
    public MapPairOperatorProvider() {
        this.supportedClasses.add(Integer.class);
        this.supportedClasses.add(Double.class);
        this.supportedClasses.add(String.class);

        operators.add(
                new MapPairOperator(
                        input -> Pair.of(input.getTupleValue(Integer.class, 0), input), Integer.class));

        operators.add(
                new MapPairOperator(
                        input -> Pair.of(input.getTupleValue(String.class, 0), input), String.class));

        operators.add(
                new MapPairOperator(
                        input -> Pair.of(input.getTupleValue(Double.class, 0), input), Double.class));

    }

    public PairFunction<DataTuple, Object, DataTuple> getMapToPair(Class<?> klass) {
        if (klass == Integer.class){
            return  input -> Pair.of(input.getTupleValue(Integer.class, 0), input);
        } else if (klass == String.class) {
            return input -> Pair.of(input.getTupleValue(String.class, 0), input);
        } else if (klass == Double.class) {
            return input -> Pair.of(input.getTupleValue(Double.class, 0), input);
        }
        throw new IllegalArgumentException(klass.toString() + " not yet supported");
    }
}

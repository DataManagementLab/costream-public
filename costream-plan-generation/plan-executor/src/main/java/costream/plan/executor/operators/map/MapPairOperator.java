package costream.plan.executor.operators.map;

import costream.plan.executor.main.Constants;
import costream.plan.executor.main.DataTuple;
import costream.plan.executor.operators.AbstractOperator;
import org.apache.storm.streams.operations.PairFunction;

import java.util.HashMap;

public class MapPairOperator extends AbstractOperator<PairFunction<DataTuple, Object, DataTuple>> {
    public MapPairOperator(PairFunction<DataTuple, Object, DataTuple> function, Class<?> klass) {
        super();
        this.function = function;
        this.klass = klass;
    }

    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put(Constants.Features.mapKey.name(), klass.getSimpleName());
        return description;
    }
}

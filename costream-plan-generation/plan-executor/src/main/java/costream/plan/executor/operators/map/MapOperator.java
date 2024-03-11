package costream.plan.executor.operators.map;

import costream.plan.executor.operators.map.functions.AbstractMapFunction;
import costream.plan.executor.operators.AbstractOperator;

import java.util.HashMap;

public class MapOperator extends AbstractOperator<AbstractMapFunction<?, ?>> {

    public MapOperator(AbstractMapFunction<?, ?> function) {
        super();
        this.function = function;
        this.klass = function.getKlass();
    }

    public HashMap<String, Object> getDescription() {
        return super.getDescription();
    }
}
package costream.plan.executor.operators.filter;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;
import costream.plan.executor.utils.RanGen;

import java.util.HashMap;
import java.util.function.BiFunction;

public class FilterOperator extends AbstractOperator<BiFunction<?, ?, Boolean>> {
    private Object literal;
    private final String filterType;

    public FilterOperator(BiFunction<?, ?, Boolean> function, String filterType, Class<?> klass) {
        super();
        this.filterType = filterType;
        this.function = function;
        this.klass = klass;
        this.literal = RanGen.generateRandomLiteral(klass);
    }

    public Object getLiteral() {
        return literal;
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", Constants.Operators.FILTER);
        description.put(Constants.Features.literal.name(), literal);
        description.put(Constants.Features.filterFunction.name(), filterType);
        description.put(Constants.Features.filterClass.name(), klass.getSimpleName());
        return description;
    }

    public void setLiteral(Object literal) {
        this.literal = literal;
    }
}


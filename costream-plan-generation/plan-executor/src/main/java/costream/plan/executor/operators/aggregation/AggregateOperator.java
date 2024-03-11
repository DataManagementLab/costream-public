package costream.plan.executor.operators.aggregation;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;
import org.apache.storm.streams.operations.CombinerAggregator;

import java.util.HashMap;

public class AggregateOperator extends AbstractOperator<CombinerAggregator<?, ?, ?>> {
    private HashMap<String, Object> windowDescription;
    private final String aggFunction;
    private String groupByClass;

    public AggregateOperator(CombinerAggregator<?, ?, ?> function, String aggFunctionName, Class<?> klass) {
        super();
        this.aggFunction = aggFunctionName;
        this.function = function;
        this.klass = klass; // refers to the class of aggregation. If we do a min over integers, this will be "Integer"
    }

    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put(Constants.Features.aggFunction.name(), aggFunction);
        description.put(Constants.Features.aggClass.name(), klass.getSimpleName());
        description.put(Constants.Features.groupByClass.name(), groupByClass);
        description.put(Constants.Features.operatorType.name(), Constants.Operators.AGGREGATE);
        if (windowDescription != null) {
            description.putAll(windowDescription);
            description.put(Constants.Features.operatorType.name(), Constants.Operators.WINDOW_AGGREGATE);
        }
        return description;
    }

    public void addWindowDescription(HashMap<String, Object> description) {
        description.remove(Constants.Features.operatorType.name());
        description.remove(Constants.Features.id.name());
        windowDescription = description;
    }

    public void setGroupByClass(String klass) {
        groupByClass = klass;
    }
}

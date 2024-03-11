package costream.plan.executor.application.smartgrid;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;

import java.util.HashMap;

public class SmartGridSpoutOperator extends AbstractOperator<Object> {
    private final int eventRate;

    public SmartGridSpoutOperator(int eventRate) {
        super();
        this.eventRate = eventRate;
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", Constants.Operators.SPOUT);
        description.put("confEventRate", eventRate);
        description.put("numInteger", 5);
        description.put("numDouble", 1);
        description.put("numString", 0);
        description.put("tupleWidthOut", 6 + 3);
        return description;
    }
}
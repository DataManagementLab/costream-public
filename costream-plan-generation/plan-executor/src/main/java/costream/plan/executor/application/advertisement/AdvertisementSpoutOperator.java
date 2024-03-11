package costream.plan.executor.application.advertisement;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;

import java.util.HashMap;

public class AdvertisementSpoutOperator extends AbstractOperator<Object> {
    private final int eventRate;

    public AdvertisementSpoutOperator(int eventRate) {
        super();
        this.eventRate = eventRate;
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", Constants.Operators.SPOUT);
        description.put("confEventRate", eventRate);
        description.put("numInteger", 2);
        description.put("numDouble", 3);
        description.put("numString", 5);
        description.put("tupleWidthOut", 10 + 3);
        return description;
    }
}

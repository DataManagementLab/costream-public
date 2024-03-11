package costream.plan.executor.application.spikedetection;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;

import java.util.HashMap;

public class SpikeDetectionSpoutOperator extends AbstractOperator<Object> {
    private final double eventRate;

    public SpikeDetectionSpoutOperator(double throughput) {
        super();
        this.eventRate = throughput;
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", Constants.Operators.SPOUT);
        description.put("confEventRate", (int) eventRate);
        description.put("numInteger", 0);
        description.put("numDouble", 1);
        description.put("numString", 2);
        description.put("tupleWidthOut", 3 + 1); // add timestamp value manually
        return description;
    }
}


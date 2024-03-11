package costream.plan.executor.operators.window;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;
import org.apache.storm.streams.windowing.Window;
import java.util.HashMap;

public class WindowOperator extends AbstractOperator<Window<?, ?>> {
    final Integer windowLength;
    Integer slidingLength;
    final String windowType;
    final String windowPolicy;

    public WindowOperator(
            String windowType,  // "tumbling" or "sliding"
            String policy,      // "duration" or "count"
            Window<?, ?> function,
            Integer windowLength,
            Integer slidingLength) {
        super();
        this.function = function;
        this.windowType = windowType;
        this.windowPolicy = policy;
        this.windowLength = windowLength;
        if (slidingLength != null) {
            this.slidingLength = slidingLength;
        }
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put(Constants.Features.windowType.name(), windowType);
        description.put(Constants.Features.windowPolicy.name(), windowPolicy);
        description.put(Constants.Features.windowLength.name(), windowLength); // toDo: Recalculate window length for counting windows!

        if (slidingLength == null) {
            description.put(Constants.Features.slidingLength.name(), windowLength);
        } else {
            description.put(Constants.Features.slidingLength.name(), slidingLength);
        }

        return description;
    }
}

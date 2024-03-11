package costream.plan.enumerator.enumerator.fixed;

import costream.plan.enumerator.CombinationSpace;
import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.executor.main.Constants;

public class LinearEnumerator extends AbstractEnumerator {

    public LinearEnumerator(CombinationSpace space) {
        super(space);
        this.mode = Constants.QueryType.FIXED.LINEAR;
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        getLinearStreamWithFilter(eventRate, tupleWidth);
    }
}

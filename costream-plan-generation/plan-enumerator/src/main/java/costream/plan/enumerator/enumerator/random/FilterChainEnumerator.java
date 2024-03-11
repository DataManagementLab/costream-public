package costream.plan.enumerator.enumerator.random;

import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;
import org.apache.commons.lang.NotImplementedException;
import org.graphstream.graph.Graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FilterChainEnumerator extends AbstractEnumerator {
    private final int jvmSize;
    private final int duration;
    private final int numOfTopos;

    private final int numOfFilters;

    public FilterChainEnumerator(List<Integer> durations, List<Integer> jvmSizes, int numOfTopos, int numOfFilters) {
        super();
        switch (numOfFilters){
            case 2:
                this.mode = Constants.QueryType.FIXED.TWO_FILTER_CHAIN;
                break;
            case 3:
                this.mode = Constants.QueryType.FIXED.THREE_FILTER_CHAIN;
                break;
            case 4:
                this.mode = Constants.QueryType.FIXED.FOUR_FILTER_CHAIN;
                break;
            default:
                throw new RuntimeException("Mode not supported");
        }

        this.numOfFilters = numOfFilters;
        this.numOfTopos = numOfTopos;
        this.duration = durations.get(0);
        this.jvmSize = jvmSizes.get(0);
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        throw new NotImplementedException();
    }

    public void buildQuery() {
        createRandomLinearStream();
        ArrayList<AbstractOperator<?>> filters = new ArrayList<>();
        while (filters.size() != numOfFilters) {
            AbstractOperator<?> operator = operatorProvider.provideOperator("filter", getOperatorIndex(false));
            if (!filters.contains(operator)) {
                currentOperatorIndex = currentOperatorIndex+1;
                operator.setId(currentQueryName + "-" + currentOperatorIndex);
                filters.add(operator);
                addQueryVertex(operator);
                addQueryEdge(curGraphHead, operator);
                curGraphHead = operator;
            }
        }
        currentOperatorIndex = currentOperatorIndex+1;
    }


    public ArrayList<Graph> buildQueryPlans() {
        for (int i = 0; i < this.numOfTopos; i++) {
            prepareNewQuery(new HashMap<String, Object>() {{
                put(Constants.QueryProperties.MODE, mode);
                put(Constants.QueryProperties.JVM_SIZE, jvmSize);
                put(Constants.QueryProperties.DURATION, duration);
            }});
            buildQuery();
            finalizeQuery();
        }
        return planList;
    }
}

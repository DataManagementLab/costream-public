package costream.plan.enumerator.enumerator.random;

import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.executor.main.Constants;
import org.apache.commons.lang.NotImplementedException;
import org.graphstream.graph.Graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static costream.plan.executor.utils.RanGen.randomBoolean;

public class RandomEnumerator extends AbstractEnumerator {
    private final int numOfTopos;
    private final int jvmSize;
    private final int duration;

    public RandomEnumerator(List<Integer> durations, List<Integer> jvmSizes, int numOfTopos, String mode) {
        super();
        this.mode = mode;
        this.numOfTopos = numOfTopos;
        if (durations.size() != 1) {
            throw new IllegalArgumentException("Exactly one argument is required for duration in random mode");
        }
        if (jvmSizes.size() != 1) {
            throw new IllegalArgumentException("Exactly one argument is required for jvmSizes in random mode");
        }
        this.duration = durations.get(0);
        this.jvmSize = jvmSizes.get(0);
    }

    private void prepareRandomQuery() {
        prepareNewQuery(new HashMap<String, Object>() {{
            put(Constants.QueryProperties.MODE, mode);
            put(Constants.QueryProperties.JVM_SIZE, jvmSize);
            put(Constants.QueryProperties.DURATION, duration);
        }});
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        throw new NotImplementedException();
    }

    @Override
    public ArrayList<Graph> buildQueryPlans() {
        // Linear query
        for (int i = 0; i < numOfTopos; i++) {
            buildRandomLinearQuery();
        }
        for (int i = numOfTopos; i < 2 * numOfTopos; i++) {
            buildRandomTwoWayJoinQuery();
        }
        for (int i = numOfTopos * 2; i < 3 * numOfTopos; i++) {
            buildRandomThreeWayJoinQuery();
        }
        return planList;
    }

    private void buildRandomLinearQuery() {
        prepareRandomQuery();
        createRandomLinearStream();
        applyRandomOperator(Constants.Operators.FILTER);
        // apply windowed aggregation, randomly use group-by or not
        boolean groupBy = randomBoolean();
        boolean filter = randomBoolean();
        boolean aggregation = randomBoolean();

        if (aggregation) {
            applyWindowedAggregation(groupBy);
        }

        if (aggregation && filter && groupBy) {
            applyRandomOperator(Constants.Operators.PAIR_FILTER);
        }

        if (aggregation && filter && !groupBy) {
            applyRandomOperator(Constants.Operators.FILTER);
        }

        finalizeQuery();
    }

    private void buildRandomTwoWayJoinQuery() {
        prepareRandomQuery();

        // first stream
        createRandomTwoWayJoinStream();
        applyRandomOperator(Constants.Operators.FILTER);
        String head0 = curGraphHead.getId();

        // second stream
        createRandomTwoWayJoinStream();
        if (randomBoolean()) {
            applyRandomOperator(Constants.Operators.FILTER);
        }
        String head1 = curGraphHead.getId();
        joinTwoStreams(head0, head1);

        // random aggregation
        if (randomBoolean()) {
            applyWindowedPairAggregation(randomBoolean());
            // random filter for aggregation
            if (randomBoolean()) {
                applyRandomOperator(Constants.Operators.PAIR_FILTER);
            }
        } else {
            // random filter for not-aggregated streams
            if (randomBoolean()) {
                applyRandomOperator(Constants.Operators.FILTER);
            }
        }
        finalizeQuery();
    }

    private void buildRandomThreeWayJoinQuery() {
        prepareRandomQuery();

        // first stream
        createRandomThreeWayJoinStream();
        applyRandomOperator(Constants.Operators.FILTER);
        String head0 = curGraphHead.getId();

        // second stream
        createRandomThreeWayJoinStream();
        applyRandomOperator(Constants.Operators.FILTER);
        String head1 = curGraphHead.getId();

        // third stream
        createRandomThreeWayJoinStream();
        if (randomBoolean()) {
            applyRandomOperator(Constants.Operators.FILTER);
        }
        String head2 = curGraphHead.getId();

        joinThreeStreams(head0, head1, head2);

        // random aggregation
        if (randomBoolean()) {
            applyWindowedPairAggregation(randomBoolean());
            // random filter for aggregation
            if (randomBoolean()) {
                applyRandomOperator(Constants.Operators.PAIR_FILTER);
            }
        } else {
            // random filter for not-aggregated streams
            if (randomBoolean()) {
                applyRandomOperator(Constants.Operators.FILTER);
            }
        }
        finalizeQuery();
    }

}

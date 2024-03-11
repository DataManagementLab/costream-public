package costream.plan.enumerator.enumerator.benchmark;

import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.executor.application.smartgrid.SmartGridSpoutOperator;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.aggregation.AggregateIterableMeanDouble;
import costream.plan.executor.operators.aggregation.AggregateOperator;
import costream.plan.executor.operators.aggregation.functions.AggregateMeanFunction;
import costream.plan.executor.operators.filter.FilterOperator;
import costream.plan.executor.operators.window.WindowOperator;
import costream.plan.executor.utils.RanGen;
import org.apache.commons.lang.NotImplementedException;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.graphstream.graph.Graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

public class SmartGridEnumerator extends AbstractEnumerator {
    private final int jvmSize;
    private final int duration;
    private final int numOfTopos;
    private final int windowLength = 6 * 1000; // 6 sec
    private final int slidingLength = 1000; // 1 sec

    public SmartGridEnumerator(List<Integer> durations, List<Integer> jvmSizes, int numOfTopos, String mode) {
        super();
        this.mode = mode;
        this.numOfTopos = numOfTopos;
        this.duration = durations.get(0);
        this.jvmSize = jvmSizes.get(0);
    }

    public void buildGlobalSmartGridQuery(int eventRate) {
        SmartGridSpoutOperator spout = new SmartGridSpoutOperator(eventRate);
        spout.setId(getOperatorIndex(true));
        addQueryVertex(spout);
        curGraphHead = spout;

        // We need a second meaningful operator to have a meaningful grouping at storms StreamBuilder.
        // Thus we add a filter operator with a selectivity of 1
        FilterOperator filterOperator = new FilterOperator(
                (BiFunction<String, String, Boolean> & Serializable) (x, y) -> !x.startsWith(y),
                "startsNotWith",
                String.class);
        filterOperator.setLiteral("---");
        filterOperator.setId(getOperatorIndex(true));
        addQueryVertex(filterOperator);
        addQueryEdge(curGraphHead, filterOperator);
        curGraphHead = filterOperator;

        WindowOperator windowOperator = new WindowOperator(
                "slidingWindow",
                "duration",
                SlidingWindows.of(BaseWindowedBolt.Duration.of(windowLength), BaseWindowedBolt.Duration.of(slidingLength)),
                windowLength,
                slidingLength);

        AggregateOperator aggOperator = new AggregateOperator(new AggregateMeanFunction<>(Double.class), "mean", Double.class);
        aggOperator.setId(getOperatorIndex(true));
        aggOperator.setGroupByClass(null);
        aggOperator.addWindowDescription(windowOperator.getDescription());
        addQueryVertex(aggOperator);
        addQueryEdge(curGraphHead, aggOperator);
        curGraphHead = aggOperator;
    }


    public void buildLocalSmartGridQuery(int eventRate) {
        // create stream
        SmartGridSpoutOperator spout = new SmartGridSpoutOperator(eventRate);
        spout.setId(getOperatorIndex(true));
        addQueryVertex(spout);
        curGraphHead = spout;

        FilterOperator filterOperator = new FilterOperator(
                (BiFunction<String, String, Boolean> & Serializable) (x, y) -> !x.startsWith(y),
                "startsNotWith",
                String.class);
        filterOperator.setLiteral("---");
        filterOperator.setId(getOperatorIndex(true));
        addQueryVertex(filterOperator);
        addQueryEdge(curGraphHead, filterOperator);
        curGraphHead = filterOperator;

        // Compute global average
        WindowOperator windowOperator = new WindowOperator(
                "slidingWindow",
                "duration",
                SlidingWindows.of(BaseWindowedBolt.Duration.of(windowLength), BaseWindowedBolt.Duration.of(slidingLength)),
                windowLength,
                slidingLength);

        AggregateOperator aggOperator = new AggregateOperator(new AggregateIterableMeanDouble(), "mean", Double.class);
        aggOperator.setId(getOperatorIndex(true));
        aggOperator.setGroupByClass(Integer.class.getSimpleName());
        aggOperator.addWindowDescription(windowOperator.getDescription());
        addQueryVertex(aggOperator);
        addQueryEdge(curGraphHead, aggOperator);
        curGraphHead = aggOperator;
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        throw new NotImplementedException();
    }

    public ArrayList<Graph> buildQueryPlans() {
        for (int i = 0; i < this.numOfTopos; i++) {
            prepareNewQuery(new HashMap<String, Object>() {{
                put(Constants.QueryProperties.MODE, mode);
                put(Constants.QueryProperties.JVM_SIZE, jvmSize);
                put(Constants.QueryProperties.DURATION, duration);
            }});

            if (this.mode.equals(Constants.QueryType.BENCHMARK.SMART_GRID_LOCAL)) {
                buildLocalSmartGridQuery(RanGen.randIntFromList(Constants.TrainingParams.EVENT_RATES));
            } else if (this.mode.equals(Constants.QueryType.BENCHMARK.SMART_GRID_GLOBAL)) {
                buildGlobalSmartGridQuery(RanGen.randIntFromList(Constants.TrainingParams.EVENT_RATES));
            }
            finalizeQuery();
        }
        return planList;
    }
}


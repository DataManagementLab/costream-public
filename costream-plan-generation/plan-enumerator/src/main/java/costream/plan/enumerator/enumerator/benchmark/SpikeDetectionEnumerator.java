package costream.plan.enumerator.enumerator.benchmark;

import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.executor.application.spikedetection.SpikeDetectionSpoutOperator;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.aggregation.AggregateIterableMeanDouble;
import costream.plan.executor.operators.aggregation.AggregateOperator;
import costream.plan.executor.operators.filter.FilterOperator;
import costream.plan.executor.operators.window.WindowOperator;
import costream.plan.executor.utils.RanGen;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.graphstream.graph.Graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

public class SpikeDetectionEnumerator extends AbstractEnumerator {
    private final int jvmSize;
    private final int duration;
    private final int numOfTopos;

    private final int WINDOW_LENGTH = 500;

    public SpikeDetectionEnumerator(List<Integer> durations, List<Integer> jvmSizes, int numOfTopos) {
        super();
        this.mode = Constants.QueryType.BENCHMARK.SPIKE_DETECTION;
        this.numOfTopos = numOfTopos;
        this.duration = durations.get(0);
        this.jvmSize = jvmSizes.get(0);
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        // create stream
        SpikeDetectionSpoutOperator spout = new SpikeDetectionSpoutOperator(eventRate);
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
                "tumblingWindow",
                "count",
                TumblingWindows.of(BaseWindowedBolt.Count.of(WINDOW_LENGTH)),
                WINDOW_LENGTH,
                null);

        AggregateOperator aggOperator = new AggregateOperator(new AggregateIterableMeanDouble(), "mean", Double.class);
        aggOperator.setId(getOperatorIndex(true));
        aggOperator.addWindowDescription(windowOperator.getDescription());
        aggOperator.setGroupByClass("Integer"); // group by device_id = sensor_id
        addQueryVertex(aggOperator);
        addQueryEdge(curGraphHead, aggOperator);
        curGraphHead = aggOperator;

        FilterOperator filterOperator2 = new FilterOperator((BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x > y, "greaterThan", Double.class);
        filterOperator2.setLiteral(27.0);
        filterOperator2.setId(getOperatorIndex(true));
        addQueryVertex(filterOperator2);
        addQueryEdge(curGraphHead, filterOperator2);
        curGraphHead = filterOperator2;
     }

    public ArrayList<Graph> buildQueryPlans() {
        for (int i = 0; i < this.numOfTopos; i++) {
            prepareNewQuery(new HashMap<String, Object>() {{
                put(Constants.QueryProperties.MODE, mode);
                put(Constants.QueryProperties.JVM_SIZE, jvmSize);
                put(Constants.QueryProperties.DURATION, duration);
            }});
            buildSingleQuery(RanGen.randIntFromList(Constants.TrainingParams.EVENT_RATES), -1, -1);
            finalizeQuery();
        }
        return planList;
    }
}


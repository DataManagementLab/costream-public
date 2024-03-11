package costream.plan.enumerator.enumerator.benchmark;

import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.executor.application.advertisement.AdvertisementSpoutOperator;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.WindowedJoinOperator;
import costream.plan.executor.operators.filter.FilterOperator;
import costream.plan.executor.operators.map.MapPairOperator;
import costream.plan.executor.operators.window.WindowOperator;
import costream.plan.executor.utils.RanGen;
import org.apache.commons.lang.NotImplementedException;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.graphstream.graph.Graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

public class AdvertisementEnumerator extends AbstractEnumerator {
    // Constants according to: https://github.com/GMAP/DSPBench/blob/master/dspbench-threads/src/main/java/org/dspbench/applications/adsanalytics/RollingCtrBolt.java#L18
    private static final Integer WINDOW_LENGTH = 4 * 1000; // time for join: 4s

    private final int jvmSize;
    private final int duration;

    private final int numOfTopos;

    public AdvertisementEnumerator(List<Integer> durations, List<Integer> jvmSizes, int numOfTopos) {
        super();
        this.mode = Constants.QueryType.BENCHMARK.ADVERTISEMENT;
        this.numOfTopos = numOfTopos;
        this.duration = durations.get(0);
        this.jvmSize = jvmSizes.get(0);

    }

    /**
     * Creating common join query
     */
    private void buildJoinAdvertisementQuery(int eventRate1, int eventRate2) {
        AdvertisementSpoutOperator spout0 = new AdvertisementSpoutOperator(eventRate1);
        spout0.setId(getOperatorIndex(true));
        addQueryVertex(spout0);
        curGraphHead = spout0;

        FilterOperator filterOperator = new FilterOperator(
                (BiFunction<String, String, Boolean> & Serializable) (x, y) -> !x.startsWith(y),
                "startsNotWith",
                String.class);
        filterOperator.setLiteral("---");
        filterOperator.setId(getOperatorIndex(true));
        addQueryVertex(filterOperator);
        addQueryEdge(curGraphHead, filterOperator);
        curGraphHead = filterOperator;
        String head0 = curGraphHead.getId();

        AdvertisementSpoutOperator spout1 = new AdvertisementSpoutOperator(eventRate2);
        spout1.setId(getOperatorIndex(true));
        addQueryVertex(spout1);
        curGraphHead = spout1;
        String head1 = curGraphHead.getId();

        // join both queries
        BaseWindowedBolt.Duration joinWindow = BaseWindowedBolt.Duration.of(WINDOW_LENGTH);
        WindowOperator windowedJoinOperator = new WindowOperator(
                "tumblingWindow",
                "duration",
                TumblingWindows.of(joinWindow),
                joinWindow.value, null);

        // Using query_id + ad_id as a join key --> first string
        MapPairOperator mapToPairOperator = new MapPairOperator(
                input -> Pair.of(input.getTupleValue(String.class, 0), input), String.class);

        HashMap<String, Object> windowedJoinDescription = new HashMap<>();
        windowedJoinDescription.put(Constants.Features.joinKeyClass.name(), mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        windowedJoinDescription.putAll(windowedJoinOperator.getDescription());
        WindowedJoinOperator windowedJoin = new WindowedJoinOperator(getOperatorIndex(true), windowedJoinDescription);

        addQueryVertex(windowedJoin);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(head0), currentGraph.getNode(windowedJoin.getId()), true);
        currentGraph.addEdge(nextEdgeIndex(), currentGraph.getNode(head1), currentGraph.getNode(windowedJoin.getId()), true);
        curGraphHead = windowedJoin;
    }

    @Override
    public void buildSingleQuery(int eventRate, int windowLength, int tupleWidth) {
        throw new NotImplementedException();
    }

    @Override
    public ArrayList<Graph> buildQueryPlans() {
        for (int i = 0; i< this.numOfTopos; i++) {
            // Creating clicks query
            prepareNewQuery(new HashMap<String, Object>() {{
                put(Constants.QueryProperties.MODE, mode);
                put(Constants.QueryProperties.JVM_SIZE, jvmSize);
                put(Constants.QueryProperties.DURATION, duration);
            }});
            buildJoinAdvertisementQuery(
                    RanGen.randIntFromList(Constants.TrainingParams.EVENT_RATES),
                    RanGen.randIntFromList(Constants.TrainingParams.EVENT_RATES));
            finalizeQuery();
        }
        return planList;
    }

}

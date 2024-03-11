package costream.plan.enumerator;

import costream.plan.enumerator.enumerator.benchmark.AdvertisementEnumerator;
import costream.plan.enumerator.enumerator.benchmark.SpikeDetectionEnumerator;
import costream.plan.enumerator.enumerator.fixed.LinearAggDurationEnumerator;
import costream.plan.enumerator.enumerator.fixed.ThreeWayJoinEnumerator;
import costream.plan.enumerator.enumerator.fixed.TwoWayJoinEnumerator;
import costream.plan.enumerator.enumerator.AbstractEnumerator;
import costream.plan.enumerator.enumerator.benchmark.SmartGridEnumerator;
import costream.plan.enumerator.enumerator.random.FilterChainEnumerator;
import costream.plan.enumerator.enumerator.fixed.LinearAggCountEnumerator;
import costream.plan.enumerator.enumerator.fixed.LinearEnumerator;
import costream.plan.enumerator.enumerator.random.RandomEnumerator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;
import costream.plan.executor.main.Constants;
import org.graphstream.graph.Graph;
import costream.plan.executor.utils.DiskReader;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import static costream.plan.executor.utils.GraphUtils.isDAG;

public class PlanEnumerator {
    @Parameter(names = {"--num", "-n"}, description = "number of queries to generate per query type",
            validateWith = PositiveInteger.class,  required = true)
    int num = 5;
    @Parameter(names = {"--output", "-o"}, description = "Directory where to log shallow query plans")
    String logDir = "query-plans";
    @Parameter(names = {"--hosts", "-h"}, description = "Maximal amount of (virtual) hosts to distribute operators on",
            validateWith = PositiveInteger.class)
    int numHosts = 5;
    @Parameter(names = {"--mode", "-m"}, description = "random or linear")
    List<String> modes = new ArrayList<>();
    @Parameter(names = {"--jvm", "-j"}, description = "Specify the JVM worker size that is assigned to the worker processes",
            validateWith = PositiveInteger.class)
    List<Integer> jvmSizes = new ArrayList<>();

    @Parameter(names = {"--dur", "-d"}, description = "execution times for the queries",
            validateWith = PositiveInteger.class)
    List<Integer> durations = new ArrayList<>();

    // Parameters for linear mode
    @Parameter(names = {"--max", "-a"}, description = "Maximal event rate for non-randomized streams",
            validateWith = PositiveInteger.class)
    int maxEventRate = 10000;

    @Parameter(names = {"--tupleWidth", "-t"}, description = "tuple widths for the queries",
            validateWith = PositiveInteger.class)
    List<Integer> tupleWidths = new ArrayList<>();
    @Parameter(names = {"--ram", "-r"}, description = "Absolute RAM to be assigned in MB",
            validateWith = PositiveInteger.class)
    List<Integer> commonRams = new ArrayList<>();
    @Parameter(names = {"--cpu", "-c"}, description = " Absolute CPU in relative unit (100 = 1CPU Core)",
            validateWith = PositiveInteger.class)
    List<Integer> commonCpus = new ArrayList<>();
    @Parameter(names = {"--network", "-nw"}, description = "Outgoing restricted network usage in MB/s per worker",
            validateWith = PositiveInteger.class)
    List<Integer> commonBandwidths = new ArrayList<>();
    @Parameter(names = {"--latency", "-lat"}, description = "Outgoing additional latency in ms per worker",
            validateWith = PositiveInteger.class)
    List<Integer> latencies = new ArrayList<>();
    @Parameter(names = {"--window", "--w"}, description = "Window size in seconds",
            validateWith = PositiveInteger.class)
    List<Integer> windowLengths = new ArrayList<>();

    public static void main(String... args) throws Exception {
        PlanEnumerator main = new PlanEnumerator();
        JCommander.newBuilder().addObject(main).build().parse(args);
        main.run();
    }

    private void setDefaultValues(){
        if (durations.size() == 0) {
            durations.add(240);             // default value is 4 min
        }
        if (jvmSizes.size() == 0) {
            jvmSizes.add(10000);            // default value is 10GB
        }
        if (commonRams.size() == 0) {
            commonRams.add(4096);           // 4096 MB default value
        }
        if (commonCpus.size() == 0) {
            commonCpus.add(400);            // 4 cores default value
        }
        if (commonBandwidths.size() == 0) {
            commonBandwidths.add(1000);     // default value is 1GBit/s
        }
        if (windowLengths.size() == 0) {
            windowLengths.add(1);           // default value is 1sec
        }
        if (tupleWidths.size() == 0) {
            tupleWidths.add(3);             // default value is 3sec
        }
        if (latencies.size() == 0) {
            latencies.add(0);               // default value is 0ms
        }
    }

    private void run() throws Exception {
        // creating equidistant event rates
        int[] eRates = new int[num];
        for (int i = 0; i < eRates.length; i++) {
            eRates[i] = (i + 1) * maxEventRate/num;
        }
        List<Integer> eventRates = Arrays.stream(eRates).boxed().collect(Collectors.toList());
        // set default values

        setDefaultValues();

        CombinationSpace space = new CombinationSpace(durations, eventRates, jvmSizes, commonRams,
                commonCpus, commonBandwidths, windowLengths, tupleWidths, latencies);

        // create query plans and placements
        PlacementEnumerator placementEnumerator = new PlacementEnumerator(numHosts);
        ArrayList<AbstractEnumerator> enumerators = new ArrayList<>();
        ArrayList<Graph> queryPlans = new ArrayList<>();
        for (String mode : modes) {
            switch (mode) {
                case Constants.QueryType.RANDOM:
                case Constants.QueryType.RANDOM_FIXED_HW:
                    enumerators.add(new RandomEnumerator(durations, jvmSizes, num, mode));
                    break;

                case Constants.QueryType.FIXED.LINEAR:
                    enumerators.add(new LinearEnumerator(space));
                    break;

                case Constants.QueryType.FIXED.LINEAR_AGG_COUNT:
                    enumerators.add(new LinearAggCountEnumerator(space));
                    break;

                case Constants.QueryType.FIXED.LINEAR_AGG_DURATION:
                    enumerators.add(new LinearAggDurationEnumerator(space));
                    break;

                case Constants.QueryType.FIXED.TWO_WAY_JOIN:
                    enumerators.add(new TwoWayJoinEnumerator(space));
                    break;

                case Constants.QueryType.FIXED.THREE_WAY_JOIN:
                    enumerators.add(new ThreeWayJoinEnumerator(space));
                    break;

                case Constants.QueryType.FIXED.TWO_FILTER_CHAIN:
                    enumerators.add(new FilterChainEnumerator(durations, jvmSizes, num, 2));
                    break;

                case Constants.QueryType.FIXED.THREE_FILTER_CHAIN:
                    enumerators.add(new FilterChainEnumerator(durations, jvmSizes, num, 3));
                    break;

                case Constants.QueryType.FIXED.FOUR_FILTER_CHAIN:
                    enumerators.add(new FilterChainEnumerator(durations, jvmSizes, num, 4));
                    break;

                case Constants.QueryType.BENCHMARK.ADVERTISEMENT:
                    enumerators.add(new AdvertisementEnumerator(durations, jvmSizes, num));
                    break;

                case Constants.QueryType.BENCHMARK.SPIKE_DETECTION:
                    enumerators.add(new SpikeDetectionEnumerator(durations, jvmSizes, num));
                    break;

                case Constants.QueryType.BENCHMARK.SMART_GRID_GLOBAL:
                case Constants.QueryType.BENCHMARK.SMART_GRID_LOCAL:
                    enumerators.add(new SmartGridEnumerator(durations, jvmSizes, num, mode));
                    break;

                default:
                    throw new RuntimeException("Mode " + mode + " is not defined. "
                            + "Please choose on of: " + Arrays.toString(Constants.QueryType.ALL));
            }
        }

        // building query plans and add placement and network info
        for (AbstractEnumerator enumerator : enumerators) {
            ArrayList<Graph> plans = enumerator.buildQueryPlans();
            placementEnumerator.addHardwareNodes(plans);
            placementEnumerator.addPlacements(plans, enumerator.getMode().equals(Constants.QueryType.RANDOM), enumerator.getMode().contains("filter"));
            placementEnumerator.setHardwareConstraints(plans);
            placementEnumerator.addNetworkFlowInfo(plans);
            queryPlans.addAll(plans);
        }

        // Check if queries are correct DAGs, no-DAG queries can happen very rarely because of early filter grouping
        ArrayList<Graph> defectPlans = new ArrayList<>();
        for (Graph query : queryPlans) {
            if (!isDAG(query)){
                defectPlans.add(query);
            }
        }
        // remove defect DAGs
        for (Graph defectPlan : defectPlans) {
            queryPlans.remove(defectPlan);
        }

        // Write graphs to disk
        Files.createDirectories(Paths.get(logDir));

        // remove old files from directory
        FileUtils.cleanDirectory(new File(logDir));

        for (Graph query : queryPlans) {
            DiskReader.writeSingleGraphToDir(query, query.getId(), logDir);
        }
        System.out.println(queryPlans.size() + " Queries have been created and written to: " + logDir);
    }
}

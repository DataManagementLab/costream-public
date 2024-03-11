package costream.plan.enumerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CombinationSpace {
    final protected List<Integer> durations;
    final protected List<Integer> eventRates;
    final protected List<Integer> jvmSizes;
    final protected List<Integer> rams;
    final protected List<Integer> cpus;
    final protected List<Integer> networkUsages;
    final protected List<Integer> windowLengths;
    final protected List<Integer> tupleWidths;
    final protected List<Integer> latencies;

    public CombinationSpace(List<Integer> durations,
                            List<Integer> eventRates,
                            List<Integer> jvmSizes,
                            List<Integer> rams,
                            List<Integer> cpus,
                            List<Integer> networkUsages,
                            List<Integer> windowLengths,
                            List<Integer> tupleWidth,
                            List<Integer> latencies) {
        this.durations = durations;
        this.eventRates = eventRates;
        this.jvmSizes = jvmSizes;
        this.rams = rams;
        this.cpus = cpus;
        this.networkUsages = networkUsages;
        this.windowLengths = windowLengths;
        this.tupleWidths = tupleWidth;
        this.latencies = latencies;
    }

    public List<List<Integer>> getCombinations() {
        return getCombinations(new ArrayList<>(Arrays.asList(
                durations, eventRates, jvmSizes, rams,
                cpus, networkUsages, windowLengths, tupleWidths, latencies)));
    }

    public static List<List<Integer>> getCombinations(List<List<Integer>> lists) {
        List<List<Integer>> combinations = new ArrayList<>();
        if (lists.size() == 0) {
            combinations.add(new ArrayList<>());
            return combinations;
        }
        List<Integer> firstList = lists.get(0);
        List<List<Integer>> remainingLists = getCombinations(lists.subList(1, lists.size()));
        for (Integer object : firstList) {
            for (List<Integer> remainingList : remainingLists) {
                List<Integer> combination = new ArrayList<>();
                combination.add(object);
                combination.addAll(remainingList);
                combinations.add(combination);
            }
        }
        return combinations;
    }
}

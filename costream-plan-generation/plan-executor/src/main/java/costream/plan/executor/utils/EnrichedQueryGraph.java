package costream.plan.executor.utils;

import org.apache.storm.generated.StormTopology;
import org.graphstream.graph.Graph;

public class EnrichedQueryGraph extends Triple<StormTopology, Graph, String> {
    public EnrichedQueryGraph(StormTopology topo, Graph graph, String name) {
        super(topo, graph, name);
    }

    public StormTopology getTopo() {
        return this.getFirst();
    }

    public Graph getGraph() {
        return this.getSecond();
    }

    public String getName() {
        return this.getThird();
    }
}

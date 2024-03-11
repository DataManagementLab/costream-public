package costream.plan.executor.main;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import costream.plan.executor.utils.EnrichedQueryGraph;
import costream.plan.executor.utils.MongoClientProvider;
import costream.plan.executor.utils.DiskReader;
import org.apache.storm.Config;
import org.graphstream.graph.Graph;
import org.json.simple.parser.ParseException;
import costream.plan.executor.utils.MongoReader;

import java.io.IOException;

/**
 * This class builds the enriched graph that is later used as training data.
 */
public class GraphBuilder {
    private final boolean localExecution;
    private final Config config;
    private Graph currentGraph;
    private MongoClient mongoClient;

    public GraphBuilder(boolean localExecution, Config config) {
        this.localExecution = localExecution;
        this.config = config;
        this.currentGraph = null;
        if (!this.localExecution) {
            mongoClient = MongoClientProvider.getMongoClient(config);
        }
    }

    @Deprecated
    public void preExecutionUpdate(EnrichedQueryGraph query, Config config) throws IOException {
        // adding hostnames and hardware features before execution
        currentGraph = DiskReader.readGraphFromDir(query.getName(), (String) this.config.get("input.path"));
        MongoDatabase db = mongoClient.getDatabase((String) config.get("mongo.database"));
        MongoReader.readHWFeaturesToGraph(
                currentGraph,
                db.getCollection((String) config.get("mongo.collection.hw.params")),
                db.getCollection((String) config.get("mongo.collection.nw.params")),
                config);

        DiskReader.writeSingleGraphToDir(
                currentGraph,
                query.getName(),
                (String) this.config.get("input.path"));
    }

    /**
     * This method takes the current Graph object and merges it with the information coming from:
     * 1. the Observations (selectivities and tuple widths)
     * 2. The Grouping of single operators ( group ID)
     * 3. The placement of the operators, (instance ID and instance size)
     * In case of local execution, a script is called in order to collect distributed logs
     *
     * @throws IOException
     */
    public void postExecutionUpdate(EnrichedQueryGraph query) throws IOException, ParseException, InterruptedException, MongoReadException {
        // As the graph gets modified by Storm-Server, it is not guaranteed that the in-memory graph contains all information.
        // However, it is reloaded from disk which always has the current version.
        currentGraph = DiskReader.readGraphFromDir(query.getName(), (String) this.config.get("input.path"));
        assert currentGraph != null;

        if (this.localExecution) {
            DiskReader.readObservationsFromFileToGraph(
                    currentGraph,
                    (String) this.config.get("observation.path")
            );

            DiskReader.addComponentsFromFileToGraph(
                    currentGraph,
                    (String) this.config.get("grouping.path")
            );

            DiskReader.addOffsetsFromFileToGraph(
                    currentGraph,
                    (String) this.config.get("offset.path")
            );

            DiskReader.addLabelsFromFileToGraph(
                    currentGraph,
                    (String) this.config.get("labels.path")
            );

        } else {
            MongoDatabase db = mongoClient.getDatabase((String) config.get("mongo.database"));

            MongoReader.readObservationsToGraph(
                    currentGraph,
                    db.getCollection((String) config.get("mongo.collection.observations")));
            MongoReader.readOffsetsToGraph(
                    currentGraph,
                    db.getCollection((String) config.get("mongo.collection.offsets")));
            MongoReader.readLabelsToGraph(
                    currentGraph,
                    db.getCollection((String) config.get("mongo.collection.labels")));
        }

        DiskReader.writeSingleGraphToDir(
                currentGraph,
                query.getName(),
                (String) this.config.get("output.path"));
    }
}

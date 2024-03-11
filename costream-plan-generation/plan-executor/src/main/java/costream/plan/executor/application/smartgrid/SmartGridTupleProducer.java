package costream.plan.executor.application.smartgrid;

import costream.plan.executor.kafka.KafkaTupleProducer;
import costream.plan.executor.utils.Triple;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.Properties;

public class SmartGridTupleProducer extends KafkaTupleProducer {
    //         declarer.declare(new Fields( "measurementTimestamp", Field.ID, Field.TIMESTAMP, Field.VALUE, Field.PROPERTY, Field.PLUG_ID, Field.HOUSEHOLD_ID, Field.HOUSE_ID));
    private final SmartPlugGenerator generator  = new SmartPlugGenerator();
    private SpoutOutputCollector collector;

    interface Field {
        String ID           = "id";
        String TIMESTAMP    = "timestamp";
        String VALUE        = "value";
        String PROPERTY     = "property";
        String PLUG_ID      = "plugId";
        String HOUSEHOLD_ID = "householdId";
        String HOUSE_ID     = "houseId";
    }

    public SmartGridTupleProducer(String queryName, String topic, Properties props, float throughputPerThread, Triple<Integer, Integer, Integer> tupleWidth) {
        super(queryName, topic, props, throughputPerThread, tupleWidth);
        generator.initialize();
    }

    @Override
    public String nextTuple() {
        StreamValues values = generator.generate();
        // Add timestamps for ingestion and creation time
        values.add(0, queryName);
        values.add(0, String.valueOf(System.currentTimeMillis()));
        values.add(0, String.valueOf(System.currentTimeMillis()));
        return values.toString();
    }
}

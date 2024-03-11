package costream.plan.executor.application.synthetic;

import costream.plan.executor.kafka.KafkaTupleProducer;
import costream.plan.executor.main.DataTuple;
import costream.plan.executor.utils.Triple;

import java.util.*;

public class SyntheticTupleProducer extends KafkaTupleProducer {

    /**
     * Creates DataTuples on a given kafka topic
     *
     * @param queryName  Query to create topics for.
     * @param topic      Topic to run against
     * @param props      KafkaConfigs for the producers
     * @param tupleWidth Tuple width to set up for this KafkaTupleProducer
     */
    public SyntheticTupleProducer(String queryName, String topic, Properties props, float throughputPerThread, Triple<Integer, Integer, Integer> tupleWidth){
        super(queryName, topic, props, throughputPerThread, tupleWidth);
    }

    @Override
    public String nextTuple() {
        DataTuple tuple = new DataTuple(this.tupleWidth, this.queryName);
        return tuple.toString();
    }
}

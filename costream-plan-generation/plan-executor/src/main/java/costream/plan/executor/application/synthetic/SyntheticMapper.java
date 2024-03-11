package costream.plan.executor.application.synthetic;


import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;
import costream.plan.executor.operators.map.functions.AbstractMapFunction;
import costream.plan.executor.utils.Triple;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Constructs a new {@link org.apache.storm.streams.operations.mappers.ValuesMapper} that extracts
 * value from a {@link Tuple} at specified indices and converts it into a custom DataTuple.
 */
public class SyntheticMapper extends AbstractMapFunction<Tuple, DataTuple> {
    final Triple<Integer, Integer, Integer> tupleWidth;
    final String queryName;

    public SyntheticMapper(Triple<Integer, Integer, Integer> tupleWidth, String queryName) {
        super(null);
        this.tupleWidth = tupleWidth;
        this.queryName = queryName;
    }

    /**
     * Converting storm {@link Tuple} into {@link DataTuple}
     *
     * @param tuple : storm tuple
     * @return DataTuple
     */
    @Override
    public DataTuple apply(Tuple tuple) throws RuntimeException {
        ArrayList<String> input = parseKafkaPayload(tuple);
        Long e2eTimestamp = Long.parseLong( input.get(0)); // retain kafka timestamp
        Long processingTimestamp = Long.parseLong(input.get(1)); // approx. here the tuple starts into the query
        input.remove(0);
        input.remove(0);

        String tupleQueryName = input.get(0); // check that incoming tuples really fit to the current query
        input.remove(0);

        if (input.size() != tupleWidth.getSum()) {
            throw new RuntimeException("Tuple" + input + " does not fit to it`s mapping config with tuple width: ["
                    + tupleWidth.getFirst()
                    + " "
                    + tupleWidth.getSecond()
                    + " "
                    + tupleWidth.getThird()
                    + "]");
        }
        if (!tupleQueryName.equals(queryName)) {
            throw new RuntimeException("Tuple queryName: " + tupleQueryName + " not equals to queryName " + queryName);
        }

        TupleContent content = new TupleContent(input, this.tupleWidth);
        return new DataTuple(content, e2eTimestamp, processingTimestamp, tupleQueryName);
    }
}

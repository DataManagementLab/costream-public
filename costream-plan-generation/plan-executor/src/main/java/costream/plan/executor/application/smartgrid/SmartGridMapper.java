package costream.plan.executor.application.smartgrid;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;
import costream.plan.executor.operators.map.functions.AbstractMapFunction;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class SmartGridMapper extends AbstractMapFunction<Tuple, DataTuple> {
    private final HashMap<String, Class<?>> parserMapping;
    private final String queryName;

    public SmartGridMapper(String queryName) {
        super(null);
        this.queryName = queryName;
        parserMapping = new LinkedHashMap<>();
        parserMapping.put("id", String.class);              // a unique identifier of the measurement [encode as String]
        parserMapping.put("timestamp", String.class);      // a timestamp of measurement [64-bit unsigned integer value]
        parserMapping.put("value", Double.class);           // the measurement [32-bit unsigned integer]
        parserMapping.put("plugId", Integer.class);         // a unique identifier (within a household) of the smart plug [32-bit unsigned integer value]
        parserMapping.put("householdId", Integer.class);    // a unique identifier of a household (within a house) where the plug is located [32-bit unsigned integer value]
        parserMapping.put("houseId", Integer.class);         // a unique identifier of a house where the household with the plug is located [32-bit unsigned integer value]
    }

    /**
     * Converting storm {@link Tuple} into {@link DataTuple}
     *
     * @param input : storm tuple
     * @return DataTuple
     */
    @Override
    public DataTuple apply(Tuple tuple) {
        ArrayList<String> input = parseKafkaPayload(tuple);
        Long e2eTimestamp = Long.parseLong( input.get(0)); // retain kafka timestamp
        Long processingTimestamp = Long.parseLong(input.get(1)); // approx. here the tuple starts into the query
        input.remove(0);
        input.remove(0);
        String tupleQueryName = input.get(0); // check that incoming tuples really fit to the current query
        input.remove(0);

        if (!tupleQueryName.equals(queryName)) {
            throw new RuntimeException("Tuple queryName: " + tupleQueryName + " not equals to queryName " + queryName);
        }

        TupleContent content = new TupleContent(input, parserMapping);
        return new DataTuple(content, e2eTimestamp, processingTimestamp, tupleQueryName);
    }
}


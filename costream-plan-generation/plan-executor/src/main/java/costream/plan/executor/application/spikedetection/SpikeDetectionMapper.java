package costream.plan.executor.application.spikedetection;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;
import costream.plan.executor.operators.map.functions.AbstractMapFunction;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class SpikeDetectionMapper extends AbstractMapFunction<Tuple, DataTuple> {

    private final HashMap<String, Class<?>> parserMapping;

    private final String queryName;

    public SpikeDetectionMapper(String queryName)  {
        super(null);
        this.queryName = queryName;
        this.parserMapping = new LinkedHashMap<>();
        // one tuple has format of: 2004-02-28 00:58:46.002832 2 19 19.7336 37.0933 71.76 2.69964
        parserMapping.put("date", String.class);
        parserMapping.put("time", String.class);
        parserMapping.put("deviceID", Integer.class);
        parserMapping.put("value1", Double.class);
        parserMapping.put("value2", Double.class);
        parserMapping.put("value3", Double.class);
        parserMapping.put("value4", Double.class);
        parserMapping.put("value5", Double.class);
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

        // Sometimes tuple are empty - replace them with -1
        for (int i=0; i< input.size(); i++) {
            String value = input.get(i);
            if (value.equals("")) {
                input.set(i, "-1");
            }
        }

        TupleContent content = new TupleContent(input, parserMapping);
        return new DataTuple(content, e2eTimestamp, processingTimestamp, tupleQueryName);
    }
}

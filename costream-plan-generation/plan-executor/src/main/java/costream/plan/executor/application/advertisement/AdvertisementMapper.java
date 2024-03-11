package costream.plan.executor.application.advertisement;

import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;
import costream.plan.executor.operators.map.functions.AbstractMapFunction;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class AdvertisementMapper extends AbstractMapFunction<Tuple, DataTuple> {
    private final HashMap<String, Class<?>> parserMapping;
    private final String queryName;
    private final String streamType;

    public AdvertisementMapper(String queryName, String streamType) {
        super(null);
        this.queryName = queryName;
        this.streamType = streamType;
        this.parserMapping = new LinkedHashMap<>();

        // See also here:
        // https://github.com/GMAP/DSPBench/blob/master/dspbench-storm/src/main/java/org/dspbench/applications/adsanalytics/AdEventParser.java
        //                  0	1	4860571499428580850	21560664	37484	2	2	2255103	317	48989	44771	490234
        // Full tuple have: CLICKS , VIEWS, DISPLAY_URL, AD_ID, ADVERTISER_ID, DEPTH, POSITION, QUERY_ID, KEYWORD_ID, TITLE_, DESC_ID, USER_ID
        // Furthermore we concatenate query_id and ad_id, as we later join over both.
        // So we only forward clicks/views (depending on the stream, ussed for aggregation), ad_id+query_id (used for join)

        parserMapping.put("clicks", Double.class);
        parserMapping.put("views", Double.class);
        parserMapping.put("displayURL", Double.class);
        parserMapping.put("adId", String.class);
        parserMapping.put("advertiserID", Integer.class);
        parserMapping.put("depth", Integer.class);
        parserMapping.put("position", Integer.class);
        parserMapping.put("queryId", String.class);
        parserMapping.put("keywordId", String.class);
        parserMapping.put("title", String.class);
        parserMapping.put("descId", String.class);
        parserMapping.put("userId", String.class);
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

        // ArrayList<String> out = new ArrayList<>();
        if (this.streamType.equals("ad-clicks")) {
            input.remove(0);
            parserMapping.remove("views");

        } else if (this.streamType.equals("ad-impressions")) {
            input.remove(1);
            parserMapping.remove("clicks");
        }
        //System.out.println(input);
        TupleContent content = new TupleContent(input, parserMapping);
        return new DataTuple(content, e2eTimestamp, processingTimestamp, queryName);
    }
}


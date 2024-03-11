package costream.plan.executor.operators;

import costream.plan.executor.main.Constants;
import costream.plan.executor.main.DataTuple;
import costream.plan.executor.main.TupleContent;
import org.apache.storm.streams.operations.ValueJoiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the basic implementation of the Windowed Join Operator. It needs an own Value Joiner that specifies how to
 * join corresponding tuples
 */
public class WindowedJoinOperator extends AbstractOperator<Object> {
    private final HashMap<String, Object> windowAndMapToPairDescr;

    public WindowedJoinOperator(String index, HashMap<String, Object> windowDescription) {
        super();
        this.id = index;
        this.windowAndMapToPairDescr = windowDescription;
    }

    public ValueJoiner<DataTuple, DataTuple, DataTuple> getValueJoiner() {
        return (value1, value2) -> {
            String queryName1 = value1.getQueryName();
            String queryName2 = value2.getQueryName();
            assert queryName1.equals(queryName2);
            long e2eTimestamp =
                    Math.max(value1.getE2ETimestamp(), value2.getE2ETimestamp());
            long processingTimestamp =
                    Math.max(value1.getProcessingTimestamp(), value2.getProcessingTimestamp());
            TupleContent content1 = value1.getTupleContent();
            TupleContent content2 = value2.getTupleContent();
            // do join
            for (Map.Entry<Class<?>, ArrayList<Object>> entry : content1.entrySet()) {
                ArrayList<Object> combinedList = content1.get(entry.getKey());
                combinedList.addAll(content2.get(entry.getKey()));
                content1.put(entry.getKey(), combinedList);
            }
            return new DataTuple(content1, e2eTimestamp, processingTimestamp, queryName1);
        };
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        windowAndMapToPairDescr.remove(Constants.Features.id.name());
        description.putAll(windowAndMapToPairDescr);
        description.put(Constants.Features.operatorType.name(), Constants.Operators.JOIN);
        return description;
    }
}

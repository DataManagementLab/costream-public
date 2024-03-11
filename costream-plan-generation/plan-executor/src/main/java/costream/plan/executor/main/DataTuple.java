package costream.plan.executor.main;

import costream.plan.executor.utils.Triple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * This class is a custom implementation of a DataTuple holding values.
 * It is internally used and provides constructors that include timestamps and meta information.
 * Furthermore, the tuple values are stored as TupleContents, which is a parallel datastructure
 * that holds the single tuple values as list sorted by their type. So each DataTuple has a list of integers,
 * one of doubles and one of strings. This is especially helpful when accessing specific tuple values, e.g. in
 * a filter operator.
 */
public class DataTuple extends Values {
    private final ArrayList<String> fields = new ArrayList<>();

    private final TupleContent tupleContent;

    /**
     * Constructor for new initialized and randomized DataTuples
     *
     * @param numTupleDatatypes: width that applies for each datatype
     */
    public DataTuple(Triple<Integer, Integer, Integer> numTupleDatatypes, String queryName) {
        // Assign timestamps. As this is an initial tuple, youngest and oldest timestamp are the same
        Long timestamp = System.currentTimeMillis();
        this.add(timestamp);
        this.fields.add("e2eTimestamp");
        this.add(timestamp);
        this.fields.add("processingTimestamp");
        this.add(queryName);
        this.fields.add("queryId");
        this.tupleContent = new TupleContent(numTupleDatatypes, false);
        this.updateTupleWithContent();
    }

    /**
     * Constructor for existing tuple contents. An additional timestamp value is added at first.
     *
     * @param tupleContents List of objects/values that are written into this DataTuple
     */
    public DataTuple(TupleContent tupleContents, Long e2eTimestamp, Long processingTimestamp, String queryName) {
        // set timestamps
        this.add(e2eTimestamp);
        this.fields.add("e2eTimestamp");
        this.add(processingTimestamp);
        this.fields.add("processingTimestamp");
        this.add(queryName);
        this.fields.add("queryName");
        this.tupleContent = tupleContents;
        this.updateTupleWithContent();
    }

    /**
     * The contents from tupleContent also need to be written into this DataTuple object to make it usable
     */
    private void updateTupleWithContent() {
        for (Map.Entry<Class<?>, ArrayList<Object>> entry : this.tupleContent.entrySet()) {
            int i = 0;
            Class<?> klass = entry.getKey();
            ArrayList<Object> classContents = this.tupleContent.get(klass);
            for (Object o : classContents) {
                this.add(klass.cast(o));
                this.fields.add((klass.getSimpleName() + "-" + i));
                i++;
            }
        }
    }

    public Triple<Integer, Integer, Integer> getNumValuesPerDataType() {
        return this.tupleContent.getNumValuesPerDataType();
    }

    public TupleContent getTupleContent() {
        TupleContent content = new TupleContent();
        ArrayList<Object> integerList = new ArrayList<>();
        ArrayList<Object> doubleList = new ArrayList<>();
        ArrayList<Object> stringList = new ArrayList<>();
        for (String field : this.fields) {
            if (field.startsWith("Integer")) {
                integerList.add(this.get(this.fields.indexOf(field)));
            }
            if (field.startsWith("String")) {
                stringList.add(this.get(this.fields.indexOf(field)));
            }
            if (field.startsWith("Double")) {
                doubleList.add(this.get(this.fields.indexOf(field)));
            }
            content.put(Integer.class, integerList);
            content.put(Double.class, doubleList);
            content.put(String.class, stringList);
        }
        return content;
    }

    public <T> T getTupleValue(Class<T> klass, int index) {
        if (index < 0 || index >= this.size()) {
            throw new IndexOutOfBoundsException(String.format("Invalid Index %s for Class %s in DataTuple", index, klass.getSimpleName()));
        }
        return (T) this.tupleContent.get(klass).get(index);
    }

    public Fields getSchema() {
        return new Fields(this.fields);
    }

    public Long getE2ETimestamp() {
        return (Long) super.get(this.fields.indexOf("e2eTimestamp"));
    }

    public Long getProcessingTimestamp() {
        return (Long) super.get(this.fields.indexOf("processingTimestamp"));
    }

    public String getQueryName() {
        return (String) super.get(this.fields.indexOf("queryName"));
    }
}

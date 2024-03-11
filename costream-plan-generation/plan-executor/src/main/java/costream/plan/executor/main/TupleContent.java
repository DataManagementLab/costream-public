package costream.plan.executor.main;

import costream.plan.executor.utils.RanGen;
import costream.plan.executor.utils.Triple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TupleContent extends LinkedHashMap<Class<?>, ArrayList<Object>> {

    public TupleContent() {
        this.put(Integer.class, new ArrayList<>());
        this.put(Double.class, new ArrayList<>());
        this.put(String.class, new ArrayList<>());
    }

    public TupleContent(ArrayList<String> tupleValues, HashMap<String, Class<?>> parserMapping) {
        /**
         * This constructor is used when a predefined, external mapping is given.
         * This is the case for external benchmarks, where the tuple order (and the datatypes) are defined externally
         */
        this.put(Integer.class, new ArrayList<>());
        this.put(Double.class, new ArrayList<>());
        this.put(String.class, new ArrayList<>());

        int index = 0;

        for (Map.Entry<String, Class<?>> singleParserMapping : parserMapping.entrySet()) {
            String value = tupleValues.get(index);
            Class<?> klass = singleParserMapping.getValue();
            if (klass == Integer.class) {
                this.get(klass).add(Integer.valueOf(value));
            } else if (klass == Double.class) {
                this.get(klass).add(Double.valueOf(value));
            } else if (klass == String.class) {
                this.get(klass).add(value);
            }
            index +=1;
        }
    }


    public TupleContent(ArrayList<String> values, Triple<Integer, Integer, Integer> tupleWidth) {
        /**
         *  This constructor is for Tuples that are read out from Kafka, where the tupleWidth is
         *  known previously to then cast the classes in a correct way.
         */
        this.put(Integer.class, new ArrayList<>());
        this.put(Double.class, new ArrayList<>());
        this.put(String.class, new ArrayList<>());

        int index = 0;
        try {
            while (index < tupleWidth.getFirst()) {
                this.get(Integer.class).add(Integer.valueOf(values.get(index)));
                index += 1;
            }
            while (index < (tupleWidth.getFirst() + tupleWidth.getSecond())) {
                this.get(Double.class).add(Double.valueOf(values.get(index)));
                index += 1;
            }

            while (index < (tupleWidth.getFirst() + tupleWidth.getSecond() + tupleWidth.getThird())) {
                this.get(String.class).add(values.get(index));
                index += 1;
            }
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }

    public TupleContent(Triple<Integer, Integer, Integer> numValuesPerDataType, boolean empty) {
        this.put(Integer.class, new ArrayList<>());
        this.put(Double.class, new ArrayList<>());
        this.put(String.class, new ArrayList<>());

        if (!empty) {
            for (int i = 0; i < numValuesPerDataType.getFirst(); i++) {
                this.get(Integer.class).add(RanGen.randIntRange(Constants.TrainingParams.INTEGER_VALUE_RANGE));
            }

            for (int i = 0; i < numValuesPerDataType.getSecond(); i++) {
                this.get(Double.class).add(RanGen.randDoubleRange(Constants.TrainingParams.DOUBLE_VALUE_RANGE));
            }

            for (int i = 0; i < numValuesPerDataType.getThird(); i++) {
                this.get(String.class).add(RanGen.randString(Constants.TrainingParams.STRING_LENGTH));
            }
        } else {
            for (int i = 0; i < numValuesPerDataType.getFirst(); i++) {
                this.get(Integer.class).add(null);
            }

            for (int i = 0; i < numValuesPerDataType.getSecond(); i++) {
                this.get(Double.class).add(null);
            }

            for (int i = 0; i < numValuesPerDataType.getThird(); i++) {
                this.get(String.class).add(null);
            }
        }
    }

    public Triple<Integer, Integer, Integer> getNumValuesPerDataType() {
        return new Triple<>(this.get(Integer.class).size(),
                this.get(Double.class).size(),
                this.get(String.class).size());
    }
}

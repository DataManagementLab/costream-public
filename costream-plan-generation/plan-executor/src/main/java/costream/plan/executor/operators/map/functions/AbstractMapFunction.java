package costream.plan.executor.operators.map.functions;

import costream.plan.executor.main.DataTuple;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public abstract class AbstractMapFunction<T, U> implements Function<T, U> {
  protected final Class<?> klass;
  LinkedHashMap<Class<?>, ArrayList<Object>> tupleContent = null;

  public AbstractMapFunction(Class<?> klass) {
    this.klass = klass;
  }

  public U apply(T input) {
    return null;
  }

  public Class<?> getKlass() {
    return klass;
  }

  public boolean checkIfClassExistsInTuple(DataTuple tuple) {
    return (tuple.getTupleContent().containsKey(String.class));
  }

  protected ArrayList<String> parseKafkaPayload(Tuple tuple) {
    if (tuple.size() == 0) {
      throw new RuntimeException("Tuple values are empty!");
    }
    return Arrays.stream(tuple.getValue(4).toString()
                    .replace("[", "")
                    .replace("]", "")
                    .replace(" ", "")
                    .split(","))
            .collect(Collectors.toCollection(ArrayList::new));
  }
}

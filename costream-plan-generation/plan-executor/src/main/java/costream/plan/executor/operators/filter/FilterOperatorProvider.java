package costream.plan.executor.operators.filter;

import costream.plan.executor.operators.AbstractOperatorProvider;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.BiFunction;

public class FilterOperatorProvider extends AbstractOperatorProvider<FilterOperator> {
    public FilterOperatorProvider() {
        this.supportedClasses.addAll(Arrays.asList(Integer.class, Double.class, String.class));
        operators.add(
                new FilterOperator(
                        (BiFunction<String, String, Boolean> & Serializable) String::startsWith,
                        "startsWith",
                        String.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<String, String, Boolean> & Serializable) String::endsWith,
                        "endsWith",
                        String.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<String, String, Boolean> & Serializable) (x, y) -> !x.startsWith(y),
                        "endsNotWith",
                        String.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<String, String, Boolean> & Serializable) (x, y) -> !x.startsWith(y),
                        "startsNotWith",
                        String.class));
        /*
          operators.add(
        new FilterOperator(
            (BiFunction<String, String, Boolean> & Serializable) String::equals,
            "equals",
            String.class));
        */
        operators.add(
                new FilterOperator(
                        (BiFunction<String, String, Boolean> & Serializable) String::contains,
                        "contains",
                        String.class));

    /*
    operators.add(
        new FilterOperator(
            (BiFunction<Integer, Integer, Boolean> & Serializable) Integer::equals,
            "equals",
            Integer.class));
     */

        operators.add(
                new FilterOperator(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> !x.equals(y),
                        "notEquals",
                        Integer.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x > y,
                        "greaterThan",
                        Integer.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x < y,
                        "lessThan",
                        Integer.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x >= y,
                        "greaterEquals",
                        Integer.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x <= y,
                        "lessEquals",
                        Integer.class));

    /*
    operators.add(
        new FilterOperator(
            (BiFunction<Double, Double, Boolean> & Serializable) Double::equals,
            "equals",
            Double.class));
     */

        operators.add(
                new FilterOperator(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> !x.equals(y),
                        "notEquals",
                        Double.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x > y,
                        "greaterThan",
                        Double.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x < y,
                        "lessThan",
                        Double.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x >= y,
                        "greaterEquals",
                        Double.class));

        operators.add(
                new FilterOperator(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x <= y,
                        "lessEquals",
                        Double.class));
    }

    public BiFunction<?, ?, Boolean> getFilterFunction(String filterType, Class<?> klass) {
        if (filterType.equals("startsWith") && klass == String.class) {
            return (BiFunction<String, String, Boolean> & Serializable) String::startsWith;
        } else if (filterType.equals("endsWith") && klass == String.class) {
            return (BiFunction<String, String, Boolean> & Serializable) String::endsWith;
        } else if (filterType.equals("startsNotWith") && klass == String.class) {
            return (BiFunction<String, String, Boolean> & Serializable) (x, y) -> !x.startsWith(y);
        } else if (filterType.equals("endsNotWith") && klass == String.class) {
            return (BiFunction<String, String, Boolean> & Serializable) (x, y) -> !x.endsWith(y);
        } else if (filterType.equals("contains") && klass == String.class) {
            return (BiFunction<String, String, Boolean> & Serializable) String::contains;
        }
        // Integer funcs
        else if (filterType.equals("notEquals") && klass == Integer.class) {
            return (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> !x.equals(y);
        } else if (filterType.equals("greaterThan") && klass == Integer.class) {
            return (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x > y;
        } else if (filterType.equals("lessThan") && klass == Integer.class) {
            return (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x < y;
        } else if (filterType.equals("greaterEquals") && klass == Integer.class) {
            return (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x >= y;
        } else if (filterType.equals("lessEquals") && klass == Integer.class) {
            return (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x <= y;
        }
        // Double funcs
        else if (filterType.equals("notEquals") && klass == Double.class) {
            return (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> !x.equals(y);
        } else if (filterType.equals("greaterThan") && klass == Double.class) {
            return (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x > y;
        } else if (filterType.equals("lessThan") && klass == Double.class) {
            return (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x < y;
        } else if (filterType.equals("greaterEquals") && klass == Double.class) {
            return (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x >= y;
        } else if (filterType.equals("lessEquals") && klass == Double.class) {
            return (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x <= y;
        }
        throw new IllegalArgumentException("There is no filter function" + filterType + "implemented for " + klass.getSimpleName());
        //return null;
    }
}

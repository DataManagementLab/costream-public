package costream.plan.executor.utils;

import costream.plan.executor.main.Constants;
import org.apache.storm.streams.Pair;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class RanGen {
    /**
     * @return Random boolean
     */
    public static boolean randomBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    /**
     * @param klass Class to get literal from
     * @return Random literal given the specification in PGConfig
     */
    public static Object generateRandomLiteral(Class<?> klass){
        Object literal;
        if (klass == Integer.class) {
            literal = randIntRange(Constants.TrainingParams.INTEGER_VALUE_RANGE);

        } else if (klass == String.class) {
            literal = randString(Constants.TrainingParams.STRING_LENGTH); // use only first letter

        } else if (klass == Double.class) {
            literal = randDoubleRange(Constants.TrainingParams.DOUBLE_VALUE_RANGE);

        } else {
            throw new RuntimeException("Class is not supported");
        }
        return literal;
    }

    /**
     * @param len Length of string
     * @return Random string with given length.
     */
    public static String randString(int len){
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, len);
    }

    public static double randDoubleRange(Pair<Double, Double> pair) {
        return randDouble(pair.getFirst(), pair.getSecond());
    }

    public static double randDouble(double min, double max) {
        return round(ThreadLocalRandom.current().nextDouble(min, max), 2);
    }

    public static int randIntRange(Pair<Integer, Integer> pair) {
        return randInt(pair.getFirst(), pair.getSecond());
    }

    /**
     * Random integer within a lower and upper bound
     */
    public static int randInt(int min, int max) {
        max = max+1; // make upperBound inclusive
        if (min >= max) {
            return 0;
        }
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    public static int randIntFromList(int[] intList) {
      return intList[ThreadLocalRandom.current().nextInt(intList.length)];
    }

    public static double randDoubleFromList(double[] doubleList) {
        return doubleList[ThreadLocalRandom.current().nextInt(doubleList.length)];
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);
    value = value * factor;
    long tmp = Math.round(value);
    return (double) tmp / factor;
  }
}



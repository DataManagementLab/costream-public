package costream.plan.executor.operators;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Abstract class for all OperatorProviders
 *
 * @param <T> Operator Class
 */
public abstract class AbstractOperatorProvider<T extends AbstractOperator<?>> {
    public final ArrayList<T> operators;
    protected final ArrayList<Class<?>> supportedClasses;

    public AbstractOperatorProvider() {
        operators = new ArrayList<>();
        supportedClasses = new ArrayList<>();
    }

    /**
     * @return A random operator from a random class that is supported
     */
    public T provideRandomOperator() {
        return provideRandomOperator(supportedClasses.get(ThreadLocalRandom.current().nextInt(supportedClasses.size())));
    }

    /**
     * Builds a list of qualifying operators and returns a random one
     *
     * @param klass Class that the operator has to apply on
     * @return random operator
     */
    public T provideRandomOperator(Class<?> klass) {
        ArrayList<T> tempList = new ArrayList<>();
        // Select correct operators.map functions for given class
        for (T operator : operators) {
            if (operator.getKlass() == klass || operator.getKlass() == null) {
                tempList.add(operator);
            }
        }
        return tempList.get(ThreadLocalRandom.current().nextInt(tempList.size()));
    }
}

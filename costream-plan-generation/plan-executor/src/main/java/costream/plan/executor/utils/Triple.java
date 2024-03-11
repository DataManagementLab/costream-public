package costream.plan.executor.utils;

import java.io.Serializable;

public class Triple<T, U, V> implements Serializable {

    private final T first;
    private final U second;
    private final V third;

    public Triple(T first, U second, V third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
    public T getFirst() { return first; }
    public U getSecond() { return second; }
    public V getThird() { return third; }

    public int getSum() {
        return (int) this.first + (int) this.second + (int) this.third;
    }
}

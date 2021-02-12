import streamql.algo.Sink;

public class QLLastSaver<T> extends Sink<T> {
    private T last;

    @Override
    public void next(T item) {
        last = item;
    }

    @Override
    public void end() {
        // do nothing
    }

    public T getLast() {
        return last;
    }
}

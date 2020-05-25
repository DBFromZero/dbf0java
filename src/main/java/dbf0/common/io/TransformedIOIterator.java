package dbf0.common.io;

import java.io.IOException;

public class TransformedIOIterator<T, R> implements IOIterator<R> {

  private final IOIterator<T> iterator;
  private final IOFunction<T, R> function;

  public TransformedIOIterator(IOIterator<T> iterator, IOFunction<T, R> function) {
    this.iterator = iterator;
    this.function = function;
  }

  @Override public boolean hasNext() throws IOException {
    return iterator.hasNext();
  }

  @Override public R next() throws IOException {
    return function.apply(iterator.next());
  }

  @Override public void close() throws IOException {
    iterator.close();
  }
}

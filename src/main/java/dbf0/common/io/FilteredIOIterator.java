package dbf0.common.io;

import java.io.IOException;
import java.util.NoSuchElementException;

public class FilteredIOIterator<T> implements IOIterator<T> {

  private final PeekingIOIterator<T> iterator;
  private final IOPredicate<T> predicate;
  private boolean hasNext;

  public FilteredIOIterator(PeekingIOIterator<T> iterator, IOPredicate<T> predicate) {
    this.iterator = iterator;
    this.predicate = predicate;
  }

  public FilteredIOIterator(IOIterator<T> iterator, IOPredicate<T> predicate) {
    this(new PeekingIOIterator<>(iterator), predicate);
  }

  @Override public boolean hasNext() throws IOException {
    if (hasNext) {
      return true;
    }
    while (iterator.hasNext()) {
      if (predicate.test(iterator.peek())) {
        hasNext = true;
        return true;
      }
      iterator.next();
    }
    return false;
  }

  @Override public T next() throws IOException {
    if (!hasNext && !hasNext()) {
      throw new NoSuchElementException();
    }
    T t = iterator.next();
    hasNext = false;
    return t;
  }

  @Override public void close() throws IOException {
    iterator.close();
  }
}

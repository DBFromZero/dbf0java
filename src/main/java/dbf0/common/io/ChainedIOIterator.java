package dbf0.common.io;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Iterator;

public class ChainedIOIterator<E> implements IOIterator<E> {

  private final IOIterator<IOIterator<E>> iterators;
  private IOIterator<E> current;

  public ChainedIOIterator(IOIterator<IOIterator<E>> iterators) {
    this.iterators = Preconditions.checkNotNull(iterators);
  }

  public ChainedIOIterator(Iterator<IOIterator<E>> iterators) {
    this(IOIterator.of(iterators));
  }

  public ChainedIOIterator(Iterable<IOIterator<E>> iterators) {
    this(iterators.iterator());
  }

  @Override public boolean hasNext() throws IOException {
    if (current == null || !current.hasNext()) {
      while (iterators.hasNext()) {
        current = Preconditions.checkNotNull(iterators.next());
        if (current.hasNext()) {
          return true;
        }
      }
      current = null;
      return false;
    }
    return true;
  }

  @Override public E next() throws IOException {
    if (!hasNext()) {
      throw new IllegalStateException("No next value");
    }
    return current.next();
  }
}

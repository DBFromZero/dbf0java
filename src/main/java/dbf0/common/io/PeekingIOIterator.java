package dbf0.common.io;

import com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;


/**
 * Modeled off of guava PeekingIterator
 */
public class PeekingIOIterator<E> implements IOIterator<E> {

  private final IOIterator<? extends E> iterator;
  private boolean hasPeeked;
  private @Nullable E peekedElement;

  public PeekingIOIterator(IOIterator<? extends E> iterator) {
    this.iterator = Preconditions.checkNotNull(iterator);
  }

  @Override public boolean hasNext() throws IOException {
    return hasPeeked || iterator.hasNext();
  }

  @Override public E next() throws IOException {
    if (!hasPeeked) {
      return iterator.next();
    }
    E result = peekedElement;
    hasPeeked = false;
    peekedElement = null;
    return result;
  }

  public E peek() throws IOException {
    if (!hasPeeked) {
      peekedElement = iterator.next();
      hasPeeked = true;
    }
    return peekedElement;
  }

  @Override public void close() throws IOException {
    iterator.close();
  }
}

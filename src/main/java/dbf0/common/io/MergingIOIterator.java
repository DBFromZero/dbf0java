package dbf0.common.io;

import java.io.IOException;
import java.util.*;


/**
 * Modeled off of guavas Iterators.mergeSorted
 * <p>
 * Modified to only call hasNext before next
 *
 * @param <T>
 */
public class MergingIOIterator<T> implements IOIterator<T> {
  private final Collection<IOIterator<? extends T>> allIterators;
  private final Queue<PeekingIOIterator<T>> queue;
  private PeekingIOIterator<T> lastTopIter = null;

  public MergingIOIterator(
      Iterable<? extends IOIterator<? extends T>> iterators,
      final Comparator<? super T> comparator) {
    queue = new PriorityQueue<>(2, (a, b) -> {
      try {
        return a.hasNext() && b.hasNext() ? comparator.compare(a.peek(), b.peek()) : -1;
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    });

    allIterators = new ArrayList<>();
    for (var iterator : iterators) {
      allIterators.add(iterator);
      queue.add(new PeekingIOIterator<>(iterator));
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    addLastTopIter();
    for (var peekingIOIterator : queue) {
      if (peekingIOIterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public T next() throws IOException {
    addLastTopIter();
    while (!queue.isEmpty()) {
      var topIter = queue.remove();
      if (topIter.hasNext()) {
        T next = topIter.next();
        lastTopIter = topIter;
        return next;
      }
    }
    throw new NoSuchElementException();
  }

  @Override public void close() throws IOException {
    for (var iterator : allIterators) {
      iterator.close();
    }
    allIterators.clear();
    queue.clear();
    lastTopIter = null;
  }

  private void addLastTopIter() {
    if (lastTopIter != null) {
      queue.add(lastTopIter);
      lastTopIter = null;
    }
  }
}

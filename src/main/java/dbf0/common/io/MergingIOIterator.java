package dbf0.common.io;

import java.io.IOException;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;


/**
 * Modeled off of guavas Iterators.mergeSorted
 * <p>
 * Modified to only call hasNext before next
 *
 * @param <T>
 */
public class MergingIOIterator<T> implements IOIterator<T> {
  final Queue<PeekingIOIterator<T>> queue;

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

    for (var iterator : iterators) {
      queue.add(new PeekingIOIterator<>(iterator));
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    for (var peekingIOIterator : queue) {
      if (peekingIOIterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public T next() throws IOException {
    while (!queue.isEmpty()) {
      var topIter = queue.remove();
      if (topIter.hasNext()) {
        T next = topIter.next();
        queue.add(topIter);
        return next;
      }
    }
    throw new NoSuchElementException();
  }
}

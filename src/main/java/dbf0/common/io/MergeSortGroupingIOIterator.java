package dbf0.common.io;

import java.io.IOException;
import java.util.*;


public class MergeSortGroupingIOIterator<T> implements IOIterator<List<T>> {
  private final Comparator<? super T> comparator;
  private final Collection<IOIterator<? extends T>> allIterators;
  private final Queue<PeekingIOIterator<T>> queue;
  private final List<PeekingIOIterator<T>> remaining;
  private final List<T> group = new ArrayList<>(16);

  public MergeSortGroupingIOIterator(
      Iterable<? extends IOIterator<? extends T>> iterators,
      Comparator<? super T> comparator) {
    this.comparator = comparator;
    queue = new PriorityQueue<>(2, (a, b) -> {
      try {
        return a.hasNext() && b.hasNext() ? comparator.compare(a.peek(), b.peek()) : -1;
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    });

    allIterators = new ArrayList<>();
    remaining = new ArrayList<>();
    for (var iterator : iterators) {
      allIterators.add(iterator);
      remaining.add(new PeekingIOIterator<>(iterator));
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    addRemaining();
    return !queue.isEmpty();
  }

  @Override
  public List<T> next() throws IOException {
    addRemaining();
    if (queue.isEmpty()) {
      throw new NoSuchElementException();
    }
    group.clear();
    PeekingIOIterator<T> firstIter = queue.remove();
    remaining.add(firstIter);
    T first = firstIter.next();
    group.add(first);
    while (!queue.isEmpty() && comparator.compare(queue.peek().peek(), first) == 0) {
      var nextIter = queue.remove();
      remaining.add(nextIter);
      group.add(nextIter.next());
    }
    return group;
  }

  @Override public void close() throws IOException {
    for (var iterator : allIterators) {
      iterator.close();
    }
    allIterators.clear();
    remaining.clear();
    queue.clear();
    group.clear();
  }

  private void addRemaining() throws IOException {
    if (!remaining.isEmpty()) {
      for (var x : remaining) {
        if (x.hasNext()) {
          queue.add(x);
        }
      }
      remaining.clear();
    }
  }
}

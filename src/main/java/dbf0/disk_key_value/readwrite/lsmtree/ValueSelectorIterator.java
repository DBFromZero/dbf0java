package dbf0.disk_key_value.readwrite.lsmtree;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import dbf0.common.ByteArrayWrapper;
import dbf0.disk_key_value.readonly.KeyValueFileIterator;
import dbf0.disk_key_value.readonly.KeyValueFileReader;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@VisibleForTesting class ValueSelectorIterator implements Iterator<Pair<ByteArrayWrapper, ByteArrayWrapper>> {

  private final PeekingIterator<KeyValueRank> sortedIterator;

  ValueSelectorIterator(Iterator<KeyValueRank> sortedIterator) {
    this.sortedIterator = Iterators.peekingIterator(sortedIterator);
  }

  @Override public boolean hasNext() {
    return sortedIterator.hasNext();
  }

  @Override public Pair<ByteArrayWrapper, ByteArrayWrapper> next() {
    var first = sortedIterator.next();
    var key = first.key;
    var highestRank = first.rank;
    var highestRankValue = first.value;
    while (sortedIterator.hasNext() && sortedIterator.peek().key.equals(key)) {
      var entry = sortedIterator.next();
      if (entry.rank > highestRank) {
        highestRank = entry.rank;
        highestRankValue = entry.value;
      }
    }
    return Pair.of(key, highestRankValue);
  }

  @VisibleForTesting
  static ValueSelectorIterator createSortedAndSelectedIterator(List<KeyValueFileReader> orderedReaders) {
    var rankedIterators = StreamUtils.zipWithIndex(orderedReaders.stream().map(KeyValueFileIterator::new))
        .map(indexedIterator -> addRank(indexedIterator.getValue(), (int) indexedIterator.getIndex()))
        .collect(Collectors.toList());
    var mergeSortedIterator = Iterators.mergeSorted(rankedIterators, Comparator.comparing(KeyValueRank::getKey));
    return new ValueSelectorIterator(mergeSortedIterator);
  }

  static Iterator<KeyValueRank> addRank(Iterator<Pair<ByteArrayWrapper, ByteArrayWrapper>> iterator, int rank) {
    return Iterators.transform(iterator, pair -> new KeyValueRank(pair.getKey(), pair.getValue(), rank));
  }

  @VisibleForTesting
  static class KeyValueRank {
    final ByteArrayWrapper key;
    final ByteArrayWrapper value;
    final int rank;

    KeyValueRank(ByteArrayWrapper key, ByteArrayWrapper value, int rank) {
      this.key = key;
      this.value = value;
      this.rank = rank;
    }

    ByteArrayWrapper getKey() {
      return key;
    }
  }
}

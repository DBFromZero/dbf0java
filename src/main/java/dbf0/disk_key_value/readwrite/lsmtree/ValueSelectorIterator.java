package dbf0.disk_key_value.readwrite.lsmtree;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import dbf0.disk_key_value.readonly.KeyValueFileIterator;
import dbf0.disk_key_value.readonly.KeyValueFileReader;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@VisibleForTesting class ValueSelectorIterator<K, V> implements Iterator<Pair<K, V>> {

  private final PeekingIterator<KeyValueRank<K, V>> sortedIterator;

  ValueSelectorIterator(Iterator<KeyValueRank<K, V>> sortedIterator) {
    this.sortedIterator = Iterators.peekingIterator(sortedIterator);
  }

  @Override public boolean hasNext() {
    return sortedIterator.hasNext();
  }

  @Override public Pair<K, V> next() {
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
  static <K, V> ValueSelectorIterator<K, V> createSortedAndSelectedIterator(List<KeyValueFileReader<K, V>> orderedReaders,
                                                                            Comparator<K> keyComparator) {
    var rankedIterators = StreamUtils.zipWithIndex(orderedReaders.stream().map(KeyValueFileIterator::new))
        .map(indexedIterator -> addRank(indexedIterator.getValue(), (int) indexedIterator.getIndex()))
        .collect(Collectors.toList());
    var mergeSortedIterator = Iterators.mergeSorted(rankedIterators, Comparator.comparing(KeyValueRank::getKey, keyComparator));
    return new ValueSelectorIterator<>(mergeSortedIterator);
  }

  static <K, V> Iterator<KeyValueRank<K, V>> addRank(Iterator<Pair<K, V>> iterator, int rank) {
    return Iterators.transform(iterator, pair -> new KeyValueRank<>(pair.getKey(), pair.getValue(), rank));
  }

  @VisibleForTesting
  static class KeyValueRank<K, V> {
    final K key;
    final V value;
    final int rank;

    KeyValueRank(K key, V value, int rank) {
      this.key = key;
      this.value = value;
      this.rank = rank;
    }

    K getKey() {
      return key;
    }
  }
}

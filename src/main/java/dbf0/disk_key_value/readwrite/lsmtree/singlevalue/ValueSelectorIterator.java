package dbf0.disk_key_value.readwrite.lsmtree.singlevalue;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.annotations.VisibleForTesting;
import dbf0.common.io.IOIterator;
import dbf0.common.io.MergingIOIterator;
import dbf0.common.io.PeekingIOIterator;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileIterator;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileReader;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@VisibleForTesting class ValueSelectorIterator<K, V> implements IOIterator<Pair<K, V>> {

  private final PeekingIOIterator<KeyValueRank<K, V>> sortedIterator;

  ValueSelectorIterator(IOIterator<KeyValueRank<K, V>> sortedIterator) {
    this.sortedIterator = new PeekingIOIterator<>(sortedIterator);
  }

  @Override public boolean hasNext() throws IOException {
    return sortedIterator.hasNext();
  }

  @Override public Pair<K, V> next() throws IOException {
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
    var mergeSortedIterator = new MergingIOIterator<>(rankedIterators, Comparator.comparing(KeyValueRank::getKey, keyComparator));
    return new ValueSelectorIterator<>(mergeSortedIterator);
  }

  static <K, V> IOIterator<KeyValueRank<K, V>> addRank(IOIterator<Pair<K, V>> iterator, int rank) {
    return iterator.map(pair -> new KeyValueRank<>(pair.getKey(), pair.getValue(), rank));
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

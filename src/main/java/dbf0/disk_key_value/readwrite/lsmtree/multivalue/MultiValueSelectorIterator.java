package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import dbf0.common.io.IOIterator;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileIterator;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@VisibleForTesting class MultiValueSelectorIterator<K, V> implements Iterator<Pair<K, List<ValueWrapper<V>>>> {

  private final PeekingIterator<KeyMultiValueRank<K, V>> sortedIterator;
  private final Comparator<V> valueComparator;

  private final List<Iterator<ValueRank<V>>> valueIterators = new ArrayList<>(8);
  private final List<ValueWrapper<V>> sortedValues = new ArrayList<>(128);
  private final MutablePair<K, List<ValueWrapper<V>>> pair = MutablePair.of(null, sortedValues);

  MultiValueSelectorIterator(Iterator<KeyMultiValueRank<K, V>> sortedIterator,
                             Comparator<V> valueComparator) {
    this.sortedIterator = Iterators.peekingIterator(sortedIterator);
    this.valueComparator = valueComparator;
  }

  @Override public boolean hasNext() {
    return sortedIterator.hasNext();
  }

  @Override public Pair<K, List<ValueWrapper<V>>> next() {
    var first = sortedIterator.next();
    var key = first.key;
    pair.setLeft(key);

    valueIterators.clear();
    while (sortedIterator.hasNext() && sortedIterator.peek().key.equals(key)) {
      valueIterators.add(sortedIterator.next().valuesIterator());
    }
    var sortedValuesIterator = Iterators.peekingIterator(Iterators.mergeSorted(valueIterators,
        (a, b) -> valueComparator.compare(a.value.getValue(), b.value.getValue())));

    sortedValues.clear();
    while (sortedValuesIterator.hasNext()) {
      var firstValue = sortedValuesIterator.next();
      var highestRank = firstValue.rank;
      var highestValue = firstValue.value;
      while (sortedValuesIterator.hasNext() &&
          sortedValuesIterator.peek().value.getValue().equals(highestValue.getValue())) {
        var next = sortedValuesIterator.next();
        if (next.rank > highestRank) {
          highestRank = next.rank;
          highestValue = next.value;
        }
      }
      if (!highestValue.isDelete()) {
        sortedValues.add(highestValue);
      }
    }
    return pair;
  }

  @VisibleForTesting
  static <K, V> MultiValueSelectorIterator<K, V> createSortedAndSelectedIterator(
      List<KeyMultiValueFileReader<K, ValueWrapper<V>>> orderedReaders,
      Comparator<K> keyComparator, Comparator<V> valueComparator) {
    var rankedIterators = StreamUtils.zipWithIndex(orderedReaders.stream().map(KeyMultiValueFileIterator::new))
        .map(indexedIterator -> addRank(indexedIterator.getValue(), (int) indexedIterator.getIndex()))
        .collect(Collectors.toList());
    var mergeSortedIterator = Iterators.mergeSorted(rankedIterators, Comparator.comparing(KeyMultiValueRank::getKey, keyComparator));
    return new MultiValueSelectorIterator<>(mergeSortedIterator, valueComparator);
  }

  static <K, V> Iterator<KeyMultiValueRank<K, V>> addRank(IOIterator<Pair<K, MultiValueResult<ValueWrapper<V>>>> iterator, int rank) {
    return iterator.transform(pair -> new KeyMultiValueRank<>(pair.getKey(), pair.getValue(), rank)).unchcecked();
  }

  @VisibleForTesting
  static final class KeyMultiValueRank<K, V> {
    final K key;
    final MultiValueResult<ValueWrapper<V>> values;
    final int rank;

    @VisibleForTesting KeyMultiValueRank(K key, MultiValueResult<ValueWrapper<V>> values, int rank) {
      this.key = key;
      this.values = values;
      this.rank = rank;
    }

    K getKey() {
      return key;
    }

    Iterator<ValueRank<V>> valuesIterator() {
      return values.valueIterator().transform(value -> new ValueRank<>(value, rank)).unchcecked();
    }
  }

  @VisibleForTesting
  static final class ValueRank<V> {
    final ValueWrapper<V> value;
    final int rank;

    @VisibleForTesting ValueRank(ValueWrapper<V> value, int rank) {
      this.value = value;
      this.rank = rank;
    }
  }
}

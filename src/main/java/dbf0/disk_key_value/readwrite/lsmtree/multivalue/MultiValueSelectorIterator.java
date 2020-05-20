package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.annotations.VisibleForTesting;
import dbf0.common.io.IOIterator;
import dbf0.common.io.MergeSortGroupingIOIterator;
import dbf0.common.io.MergingIOIterator;
import dbf0.common.io.PeekingIOIterator;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileIterator;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@VisibleForTesting class MultiValueSelectorIterator<K, V> implements IOIterator<Pair<K, List<ValueWrapper<V>>>> {

  private final IOIterator<List<KeyMultiValueRank<K, V>>> sortedIterator;
  private final Comparator<V> valueComparator;

  private final List<IOIterator<ValueRank<V>>> rankedValuesIterators = new ArrayList<>(8);
  private final List<ValueWrapper<V>> sortedValues = new ArrayList<>(128);
  private final MutablePair<K, List<ValueWrapper<V>>> pair = MutablePair.of(null, sortedValues);

  MultiValueSelectorIterator(IOIterator<List<KeyMultiValueRank<K, V>>> sortedIterator,
                             Comparator<V> valueComparator) {
    this.sortedIterator = sortedIterator;
    this.valueComparator = valueComparator;
  }

  @Override public boolean hasNext() throws IOException {
    return sortedIterator.hasNext();
  }

  @Override public Pair<K, List<ValueWrapper<V>>> next() throws IOException {
    var keyEntries = sortedIterator.next();
    pair.setLeft(keyEntries.get(0).key);
    sortedValues.clear();
    if (keyEntries.size() == 1) {
      var iterator = keyEntries.get(0).values.valueIterator();
      while (iterator.hasNext()) {
        var value = iterator.next();
        if (!value.isDelete()) {
          sortedValues.add(value);
        }
      }
    } else {
      addSortedValuesByRank(keyEntries);
    }
    keyEntries.clear();

    return pair;
  }

  private void addSortedValuesByRank(List<KeyMultiValueRank<K, V>> keyEntries) throws IOException {
    for (var keyEntry : keyEntries) {
      rankedValuesIterators.add(keyEntry.rankedValuesIterator());
    }
    var sortedValuesIterator = new PeekingIOIterator<>(new MergingIOIterator<>(rankedValuesIterators,
        (a, b) -> valueComparator.compare(a.value.getValue(), b.value.getValue())));

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
    rankedValuesIterators.clear();
  }

  static <K, V> MultiValueSelectorIterator<K, V> createSortedAndSelectedIterator(
      List<KeyMultiValueFileReader<K, ValueWrapper<V>>> orderedReaders,
      Comparator<K> keyComparator, Comparator<V> valueComparator) {
    return new MultiValueSelectorIterator<>(createSortedIterator(orderedReaders, keyComparator), valueComparator);
  }

  @VisibleForTesting
  static <K, V> MergeSortGroupingIOIterator<KeyMultiValueRank<K, V>>
  createSortedIterator(List<KeyMultiValueFileReader<K, ValueWrapper<V>>> orderedReaders, Comparator<K> keyComparator) {
    var rankedIterators = StreamUtils.zipWithIndex(orderedReaders.stream().map(KeyMultiValueFileIterator::new))
        .map(indexedIterator -> addRank(indexedIterator.getValue(), (int) indexedIterator.getIndex()))
        .collect(Collectors.toList());
    return new MergeSortGroupingIOIterator<>(rankedIterators, Comparator.comparing(KeyMultiValueRank::getKey, keyComparator));
  }

  private static <K, V> IOIterator<KeyMultiValueRank<K, V>> addRank(
      IOIterator<Pair<K, MultiValueResult<ValueWrapper<V>>>> iterator, int rank) {
    return iterator.map(pair -> new KeyMultiValueRank<>(pair.getKey(), pair.getValue(), rank));
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

    MultiValueResult<ValueWrapper<V>> getValues() {
      return values;
    }

    int getRank() {
      return rank;
    }

    IOIterator<ValueRank<V>> rankedValuesIterator() {
      return values.valueIterator().map(value -> new ValueRank<>(value, rank));
    }
  }

}

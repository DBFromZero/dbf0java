package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.io.IOIterator;
import dbf0.common.io.MergingIOIterator;
import dbf0.common.io.PeekingIOIterator;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

class ResultRemovingDeletesMultipleSource<V> implements MultiValueReadWriteStorage.Result<V> {

  // Ordered newest to oldest
  // Each reader is sorted by value
  private final List<MultiValueResult<ValueWrapper<V>>> results;
  private final Comparator<V> comparator;

  public ResultRemovingDeletesMultipleSource(List<MultiValueResult<ValueWrapper<V>>> results,
                                             Comparator<V> comparator) {
    this.results = results;
    this.comparator = comparator;
  }

  @Override public IOIterator<V> iterator() {
    List<IOIterator<ValueRank<V>>> rankedValueIterators = new ArrayList<>(results.size());
    int rank = results.size();
    for (var result : results) {
      rankedValueIterators.add(addRank(result, --rank));
    }
    var merged = new MergingIOIterator<>(rankedValueIterators,
        (a, b) -> comparator.compare(a.value.getValue(), b.value.getValue()));
    var peeking = new PeekingIOIterator<>(merged);
    var selectingHighestRank = new IOIterator<ValueWrapper<V>>() {
      @Override public boolean hasNext() throws IOException {
        return peeking.hasNext();
      }

      @Override public ValueWrapper<V> next() throws IOException {
        var first = peeking.next();
        int highestRank = first.rank;
        ValueWrapper<V> highestValue = first.value;
        while (peeking.hasNext() && peeking.peek().value.getValue().equals(highestValue.getValue())) {
          var next = peeking.next();
          if (next.rank > highestRank) {
            highestRank = next.rank;
            highestValue = next.value;
          }
        }
        return highestValue;
      }
    };

    return selectingHighestRank.filter(v -> !v.isDelete()).map(ValueWrapper::getValue);
  }

  @Override public void close() throws IOException {
    for (var r : results) {
      r.close();
    }
  }

  @Override public int maxSize() {
    int total = 0;
    for (var r : results) {
      total += r.count();
    }
    return total;
  }

  private IOIterator<ValueRank<V>> addRank(MultiValueResult<ValueWrapper<V>> result, int rank) {
    return result.valueIterator().map(v -> new ValueRank<>(v, rank));
  }
}

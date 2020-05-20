package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.io.IOIterator;
import dbf0.disk_key_value.readonly.multivalue.CombinedMultiValueResult;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MultiValueResultFilter<V> implements MultiValueResult<V> {

  private final MultiValueResult<ValueWrapper<V>> inner;
  private FilteringIterator<V> filteringIterator;

  public MultiValueResultFilter(MultiValueResult<ValueWrapper<V>> inner) {
    this.inner = inner;
  }

  @Nullable public static <V> MultiValueResult<V> create(List<MultiValueResult<ValueWrapper<V>>> results) {
    if (results == null || results.isEmpty()) {
      return null;
    } else if (results.size() == 1) {
      return new MultiValueResultFilter<>(results.get(0));
    } else {
      return new MultiValueResultFilter<>(new CombinedMultiValueResult<>(results));
    }
  }

  @Override public int count() {
    return inner.count();
  }

  @Override public int remaining() {
    return inner.remaining();
  }

  @Override public V readValue() throws IOException {
    return valueIterator().next();
  }

  @Override public IOIterator<V> valueIterator() {
    if (filteringIterator == null) {
      filteringIterator = new FilteringIterator<>(inner.valueIterator());
    }
    return filteringIterator;
  }

  @Override public void close() throws IOException {
    inner.close();
  }

  private static class FilteringIterator<V> implements IOIterator<V> {
    private final IOIterator<ValueWrapper<V>> inner;
    private final Set<V> deletedValues = new HashSet<>(8);
    private V value;

    public FilteringIterator(IOIterator<ValueWrapper<V>> inner) {
      this.inner = inner;
    }

    @Override public boolean hasNext() throws IOException {
      while (value == null && inner.hasNext()) {
        var v = inner.next();
        if (v.isDelete()) {
          deletedValues.add(v.getValue());
        } else if (!deletedValues.contains(v.getValue())) {
          value = v.getValue();
        }
      }
      return value != null;
    }

    @Override public V next() throws IOException {
      if (value == null) {
        if (!hasNext()) {
          throw new IllegalStateException("iterator is empty");
        }
      }
      var v = value;
      value = null;
      return v;
    }
  }
}

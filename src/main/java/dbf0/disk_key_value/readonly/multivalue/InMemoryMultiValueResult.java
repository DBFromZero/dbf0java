package dbf0.disk_key_value.readonly.multivalue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import dbf0.common.io.IOIterator;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class InMemoryMultiValueResult<V> implements MultiValueResult<V> {

  @Nullable private Collection<V> values;
  private int remaining;
  @Nullable private WrappingIterator iterator;

  public InMemoryMultiValueResult(@NotNull Collection<V> values) {
    this.values = Preconditions.checkNotNull(values);
    remaining = values.size();
  }

  @Override public int count() {
    Preconditions.checkState(values != null, "already closed");
    return values.size();
  }

  @Override public int remaining() {
    Preconditions.checkState(values != null, "already closed");
    return remaining;
  }

  @Override public V readValue() throws IOException {
    return valueIterator().next();
  }

  @Override public IOIterator<V> valueIterator() {
    Preconditions.checkState(values != null, "already closed");
    if (iterator == null) {
      iterator = new WrappingIterator(values.iterator());
    }
    return iterator;
  }

  @Override public void close() throws IOException {
    values = null;
    iterator = null;
  }

  private class WrappingIterator implements IOIterator<V> {

    private final Iterator<V> iterator;

    public WrappingIterator(Iterator<V> iterator) {
      this.iterator = iterator;
    }

    @Override public boolean hasNext() {
      Preconditions.checkState(values != null, "already closed");
      return remaining > 0;
    }

    @Override public V next() {
      Preconditions.checkState(values != null, "already closed");
      V value = iterator.next();
      remaining--;
      return value;
    }
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("values", values)
        .add("remaining", remaining)
        .toString();
  }
}

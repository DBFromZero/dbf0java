package dbf0.disk_key_value.readonly.multivalue;

import com.google.common.base.Preconditions;
import dbf0.common.io.ChainedIOIterator;
import dbf0.common.io.IOIterator;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

public class CombinedMultiValueResult<V> implements MultiValueResult<V> {

  @Nullable private Collection<MultiValueResult<V>> components;
  @Nullable private IOIterator<V> iterator;

  public CombinedMultiValueResult(@NotNull Collection<MultiValueResult<V>> components) {
    this.components = Preconditions.checkNotNull(components);
  }

  @Override public int count() {
    Preconditions.checkState(components != null, "already closed");
    int total = 0;
    for (var component : components) {
      total += component.count();
    }
    return total;
  }

  @Override public int remaining() {
    Preconditions.checkState(components != null, "already closed");
    int total = 0;
    for (var component : components) {
      total += component.remaining();
    }
    return total;
  }

  @Override public V readValue() throws IOException {
    return valueIterator().next();
  }

  @Override public IOIterator<V> valueIterator() {
    if (iterator == null) {
      Preconditions.checkState(components != null, "already closed");
      iterator = new ChainedIOIterator<>(components.stream().map(MultiValueResult::valueIterator).iterator());
    }
    return iterator;
  }

  @Override public void close() throws IOException {
    if (components != null) {
      for (MultiValueResult<V> component : components) {
        component.close();
      }
      components.clear();
      components = null;
      iterator = null;
    }
  }
}

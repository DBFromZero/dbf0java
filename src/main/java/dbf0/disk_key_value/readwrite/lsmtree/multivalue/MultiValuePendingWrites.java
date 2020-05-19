package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.TreeMultimap;
import dbf0.disk_key_value.readwrite.lsmtree.base.PendingWrites;
import org.jetbrains.annotations.NotNull;

public class MultiValuePendingWrites<K, V> implements PendingWrites<TreeMultimap<K, ValueWrapper<V>>> {

  private final TreeMultimap<K, ValueWrapper<V>> writes;

  public MultiValuePendingWrites(@NotNull TreeMultimap<K, ValueWrapper<V>> writes) {
    this.writes = Preconditions.checkNotNull(writes);
  }

  @Override @NotNull public TreeMultimap<K, ValueWrapper<V>> getWrites() {
    return writes;
  }

  @Override public int size() {
    return writes.size();
  }
}

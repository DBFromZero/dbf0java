package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.readwrite.lsmtree.base.PendingWrites;
import org.jetbrains.annotations.NotNull;

public class MultiValuePendingWrites<K, V> implements PendingWrites<PutAndDeletes<K, V>> {

  private final PutAndDeletes<K, V> writes;

  public MultiValuePendingWrites(@NotNull PutAndDeletes<K, V> writes) {
    this.writes = Preconditions.checkNotNull(writes);
  }

  @Override @NotNull public PutAndDeletes<K, V> getWrites() {
    return writes;
  }

  @Override public int size() {
    return writes.size();
  }
}

package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting final class ValueRank<V> {
  final ValueWrapper<V> value;
  final int rank;

  @VisibleForTesting ValueRank(ValueWrapper<V> value, int rank) {
    this.value = value;
    this.rank = rank;
  }
}

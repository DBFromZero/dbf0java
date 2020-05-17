package dbf0.disk_key_value.readwrite.lsmtree;

import dbf0.disk_key_value.readwrite.log.WriteAheadLogWriter;

import javax.annotation.Nullable;
import java.util.Map;

class PendingWritesAndLog<K, V> {

  final Map<K, V> writes;
  @Nullable final WriteAheadLogWriter logWriter;

  public PendingWritesAndLog(Map<K, V> writes,
                             @Nullable WriteAheadLogWriter logWriter) {
    this.writes = writes;
    this.logWriter = logWriter;
  }
}

package dbf0.disk_key_value.readwrite.lsmtree;

import dbf0.common.ByteArrayWrapper;
import dbf0.disk_key_value.readwrite.log.WriteAheadLogWriter;

import javax.annotation.Nullable;
import java.util.Map;

class PendingWritesAndLog {

  final Map<ByteArrayWrapper, ByteArrayWrapper> writes;
  @Nullable final WriteAheadLogWriter logWriter;

  public PendingWritesAndLog(Map<ByteArrayWrapper, ByteArrayWrapper> writes,
                             @Nullable WriteAheadLogWriter logWriter) {
    this.writes = writes;
    this.logWriter = logWriter;
  }
}

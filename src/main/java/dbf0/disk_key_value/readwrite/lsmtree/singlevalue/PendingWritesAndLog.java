package dbf0.disk_key_value.readwrite.lsmtree.singlevalue;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import dbf0.disk_key_value.readwrite.log.WriteAheadLogWriter;
import dbf0.disk_key_value.readwrite.lsmtree.base.PendingWrites;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

public class PendingWritesAndLog<K, V> implements PendingWrites<Map<K, V>> {

  private final Map<K, V> writes;
  @Nullable private final WriteAheadLogWriter logWriter;
  @Nullable private final WriteAheadLog<?> writeAheadLog;

  public PendingWritesAndLog(@NotNull Map<K, V> writes,
                             @Nullable WriteAheadLog<?> writeAheadLog,
                             @Nullable WriteAheadLogWriter logWriter) {
    this.writes = Preconditions.checkNotNull(writes);
    Preconditions.checkArgument((logWriter == null) == (writeAheadLog == null));
    this.logWriter = logWriter;
    this.writeAheadLog = writeAheadLog;
  }

  public PendingWritesAndLog(@NotNull Map<K, V> writes) {
    this(writes, null, null);
  }

  @Override @NotNull public Map<K, V> getWrites() {
    return writes;
  }

  @Nullable public WriteAheadLogWriter getLogWriter() {
    return logWriter;
  }

  @Override public int size() {
    return writes.size();
  }

  @Override public void freeWriteAheadLog() throws IOException {
    if (writeAheadLog != null) {
      writeAheadLog.freeWriter(logWriter.getName());
    }
  }
}

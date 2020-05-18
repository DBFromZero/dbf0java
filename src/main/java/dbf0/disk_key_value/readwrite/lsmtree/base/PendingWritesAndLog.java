package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.readwrite.log.WriteAheadLogWriter;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

public class PendingWritesAndLog<W> {

  private final W writes;
  @Nullable private final WriteAheadLogWriter logWriter;

  public PendingWritesAndLog(@NotNull W writes,
                             @Nullable WriteAheadLogWriter logWriter) {
    this.writes = Preconditions.checkNotNull(writes);
    this.logWriter = logWriter;
  }

  @NotNull public W getWrites() {
    return writes;
  }

  @Nullable public WriteAheadLogWriter getLogWriter() {
    return logWriter;
  }
}

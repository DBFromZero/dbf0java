package dbf0.disk_key_value.readwrite.log;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.PrefixIo;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class WriteAheadLogWriter implements Closeable {

  private final String name;
  private OutputStream outputStream;
  private final LogSynchronizer logSynchronizer;

  public WriteAheadLogWriter(String name, OutputStream outputStream, LogSynchronizer logSynchronizer) {
    this.name = name;
    this.outputStream = outputStream;
    this.logSynchronizer = logSynchronizer;
  }

  @NotNull public String getName() {
    return name;
  }

  public synchronized void logPut(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(outputStream != null, "%s is already closed", name);
    outputStream.write(WriteAheadLogConstants.PUT);
    PrefixIo.writeBytes(outputStream, key);
    PrefixIo.writeBytes(outputStream, value);
    logSynchronizer.registerLog();
  }

  public synchronized void logDelete(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(outputStream != null, "%s is already closed", name);
    outputStream.write(WriteAheadLogConstants.DELETE);
    PrefixIo.writeBytes(outputStream, key);
    logSynchronizer.registerLog();
  }

  @Override public synchronized void close() throws IOException {
    outputStream.close();
    outputStream = null;
  }
}

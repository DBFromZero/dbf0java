package dbf0.disk_key_value.readwrite.log;

import dbf0.disk_key_value.io.FileOperations;

import java.io.IOException;
import java.io.OutputStream;

public class ImmediateLogSynchronizer<T extends OutputStream> implements LogSynchronizer {

  private final FileOperations<T> fileOperations;
  private final T outputStream;

  public ImmediateLogSynchronizer(FileOperations<T> fileOperations, T outputStream) {
    this.fileOperations = fileOperations;
    this.outputStream = outputStream;
  }

  @Override public void registerLog() throws IOException {
    fileOperations.sync(outputStream);
  }

  @Override public void close() throws IOException {
    // do nothing, all of our writes have been sync'd
  }

  public static <T extends OutputStream> LogSynchronizer.Factory<T> factory() {
    return ImmediateLogSynchronizer::new;
  }
}

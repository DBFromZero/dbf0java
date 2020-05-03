package dbf0.disk_key_value.readwrite.log;

import dbf0.disk_key_value.io.FileOperations;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public interface LogSynchronizer extends Closeable {

  void registerLog() throws IOException;

  interface Factory<T extends OutputStream> {
    LogSynchronizer create(FileOperations<T> fileOperations, T outputStream) throws IOException;
  }
}

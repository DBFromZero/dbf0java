package dbf0.disk_key_value.io;

import java.io.IOException;
import java.io.OutputStream;

public interface FileOperations<T extends OutputStream> extends ReadOnlyFileOperations {

  T createAppendOutputStream() throws IOException;

  void sync(T outputStream) throws IOException;

  void delete() throws IOException;

  OverWriter<T> createOverWriter() throws IOException;

  interface OverWriter<T extends OutputStream> {
    T getOutputStream();

    void commit() throws IOException;

    void abort();
  }
}

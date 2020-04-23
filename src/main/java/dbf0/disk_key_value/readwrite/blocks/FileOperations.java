package dbf0.disk_key_value.readwrite.blocks;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileOperations<T extends OutputStream> {

  boolean exists() throws IOException;

  T createAppendOutputStream() throws IOException;

  void sync(T outputStream) throws IOException;

  InputStream createInputStream() throws IOException;

  OverWriter<T> createOverWriter() throws IOException;

  interface OverWriter<T extends OutputStream> {
    T getOutputStream();

    void commit() throws IOException;

    void abort();
  }
}

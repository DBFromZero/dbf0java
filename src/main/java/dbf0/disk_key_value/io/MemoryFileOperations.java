
package dbf0.disk_key_value.io;


import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;

import java.io.*;

/**
 * Primarily exists for testing without having to create temporary files
 */
public class MemoryFileOperations implements FileOperations<MemoryFileOperations.MemoryOutputStream> {

  private MemoryOutputStream currentOutput;

  public MemoryFileOperations() {
  }

  public MemoryFileOperations(ByteArrayWrapper initialValue) throws IOException {
    createAppendOutputStream().write(initialValue.getArray());
  }

  @Override public MemoryOutputStream createAppendOutputStream() {
    if (currentOutput == null) {
      currentOutput = new MemoryOutputStream();
    }
    return currentOutput;
  }

  @Override public void sync(MemoryOutputStream outputStream) {
  }

  @Override public OverWriter<MemoryOutputStream> createOverWriter() throws IOException {
    return new MemoryOverWriter();
  }

  @Override public boolean exists() throws IOException {
    return currentOutput != null;
  }

  @Override public InputStream createInputStream() throws IOException {
    if (currentOutput == null) {
      throw new FileNotFoundException();
    }
    return currentOutput.createView();
  }

  public int size() {
    return currentOutput == null ? 0 : currentOutput.size();
  }

  public static class MemoryOutputStream extends ByteArrayOutputStream {

    public MemoryOutputStream() {
    }

    private ByteArrayInputStream createView() {
      return new ByteArrayInputStream(buf, 0, count);
    }
  }

  private class MemoryOverWriter implements OverWriter<MemoryOutputStream> {

    MemoryOutputStream outputStream = new MemoryOutputStream();

    @Override public MemoryOutputStream getOutputStream() {
      Preconditions.checkState(outputStream != null);
      return outputStream;
    }

    @Override public void commit() throws IOException {
      Preconditions.checkState(outputStream != null);
      currentOutput = outputStream;
      outputStream = null;
    }

    @Override public void abort() {
      Preconditions.checkState(outputStream != null);
      outputStream = null;
    }
  }
}

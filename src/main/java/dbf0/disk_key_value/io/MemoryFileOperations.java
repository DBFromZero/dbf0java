
package dbf0.disk_key_value.io;


import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;

import java.io.*;

/**
 * Primarily exists for testing without having to create temporary files
 */
public class MemoryFileOperations implements FileOperations<MemoryFileOperations.MemoryOutputStream> {

  private final String name;
  private MemoryOutputStream currentOutput;

  public MemoryFileOperations() {
    this.name = "<unnamed>";
  }

  public MemoryFileOperations(ByteArrayWrapper initialValue) throws IOException {
    this();
    createAppendOutputStream().write(initialValue.getArray());
  }

  public MemoryFileOperations(String name) {
    this.name = name;
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("exists", exists())
        .add("length", exists() ? length() : -1)
        .toString();
  }

  @Override public MemoryOutputStream createAppendOutputStream() {
    if (currentOutput == null) {
      currentOutput = new MemoryOutputStream();
    }
    return currentOutput;
  }

  @Override public void sync(MemoryOutputStream outputStream) {
  }

  @Override public void delete() throws IOException {
    if (currentOutput == null) {
      throw new IOException("memory file " + name + " does not exist");
    }
    currentOutput = null;
  }

  @Override public OverWriter<MemoryOutputStream> createOverWriter() throws IOException {
    return new MemoryOverWriter();
  }

  @Override public boolean exists() {
    return currentOutput != null;
  }

  @Override public InputStream createInputStream() throws IOException {
    if (currentOutput == null) {
      throw new FileNotFoundException();
    }
    return currentOutput.createView();
  }

  @Override public long length() {
    return currentOutput == null ? 0 : currentOutput.size();
  }

  public static class MemoryOutputStream extends ByteArrayOutputStream {

    public MemoryOutputStream() {
    }

    private ByteArrayInputStream createView() {
      return new ByteArrayInputStream(buf, 0, count);
    }
  }

  class MemoryOverWriter implements OverWriter<MemoryOutputStream> {

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
      outputStream = null;
    }
  }
}

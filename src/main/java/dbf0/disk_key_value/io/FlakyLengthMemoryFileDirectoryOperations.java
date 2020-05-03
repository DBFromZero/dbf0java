package dbf0.disk_key_value.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class FlakyLengthMemoryFileDirectoryOperations extends MemoryFileOperations {

  private final int flakeLength;

  public FlakyLengthMemoryFileDirectoryOperations(int flakeLength) {
    this.flakeLength = flakeLength;
  }

  public FlakyLengthMemoryFileDirectoryOperations(String name, int flakeLength) {
    super(name);
    this.flakeLength = flakeLength;
  }

  @Override protected @NotNull MemoryFileOperations.MemoryOutputStream createMemoryOutputStream() {
    return new FlakyMemoryOutputStream(flakeLength);
  }

  public static class FlakyMemoryOutputStream extends MemoryOutputStream {

    private final int flakeLength;

    public FlakyMemoryOutputStream(int flakeLength) {
      this.flakeLength = flakeLength;
    }

    @Override public void write(int b) throws IOException {
      if (size() == flakeLength) {
        throw new IOException("flaking");
      }
      super.write(b);
    }

    @Override public void write(@NotNull byte[] b, int off, int len) throws IOException {
      if (size() + len >= flakeLength) {
        throw new IOException("flaking");
      }
      super.write(b, off, len);
    }
  }
}

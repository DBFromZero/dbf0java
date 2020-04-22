package dbf0.common;

import org.jetbrains.annotations.NotNull;

import java.io.*;

public class PositionTrackingStream extends BufferedOutputStream {

  public static final int DEFAULT_BUFFER_SIZE = 0x8000;
  private long position = 0;

  public PositionTrackingStream(OutputStream outputStream, int bufferSize) {
    super(outputStream, bufferSize);
  }

  public PositionTrackingStream(OutputStream outputStream) {
    this(outputStream, DEFAULT_BUFFER_SIZE);
  }

  public PositionTrackingStream(String path) throws FileNotFoundException {
    this(new FileOutputStream(path), 0x8000);
  }

  public long getPosition() {
    return position;
  }

  @Override public void write(int b) throws IOException {
    super.write(b);
    position += 1;
  }

  @Override public void write(@NotNull byte[] b, int off, int len) throws IOException {
    super.write(b, off, len);
    position += len;
  }

  // Note that write(byte[] b) is not override because that just calls write(byte[] b, int off, int len)
}

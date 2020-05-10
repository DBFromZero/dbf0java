package dbf0.common.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

public class SizePrefixedDeserializer<T> implements Deserializer<T> {

  private final Deserializer<T> deserializer;
  private final boolean safe;

  public SizePrefixedDeserializer(Deserializer<T> deserializer, boolean safe) {
    this.deserializer = deserializer;
    this.safe = safe;
  }

  @NotNull @Override public T deserialize(InputStream s) throws IOException {
    int size = IOUtil.readVariableLengthUnsignedInt(s);
    if (!safe) {
      return deserializer.deserialize(s);
    }
    var view = new StreamView(s, size);
    T x = deserializer.deserialize(view);
    if (view.bytesRead != size) {
      throw new RuntimeException("Failed to read full size " + size + ". Only read " + view.bytesRead);
    }
    return x;
  }

  @Override public void skipDeserialize(InputStream s) throws IOException {
    IOUtil.skip(s, IOUtil.readVariableLengthUnsignedInt(s));
  }

  private static class StreamView extends InputStream {
    private final InputStream stream;
    private final int viewSize;
    private int bytesRead = 0;

    public StreamView(InputStream stream, int viewSize) {
      super();
      this.stream = stream;
      this.viewSize = viewSize;
    }

    @Override public int read() throws IOException {
      bytesRead++;
      checkBytesRead();
      return stream.read();
    }

    @Override public int read(@NotNull byte[] b) throws IOException {
      bytesRead += b.length;
      checkBytesRead();
      return stream.read(b);
    }

    @Override public int read(@NotNull byte[] b, int off, int len) throws IOException {
      bytesRead += len;
      checkBytesRead();
      return stream.read(b, 0, len);
    }

    @Override public long skip(long n) throws IOException {
      bytesRead += n;
      checkBytesRead();
      return super.skip(n);
    }

    @Override public byte[] readAllBytes() throws IOException {
      throw new RuntimeException("Should not call readAllBytes");
    }

    @Override public int available() throws IOException {
      throw new RuntimeException("Should not check available");
    }

    @Override public void close() throws IOException {
      throw new RuntimeException("Should not close stream");
    }

    private void checkBytesRead() {
      if (bytesRead > viewSize) {
        throw new RuntimeException("Attempted to read " + (bytesRead - viewSize) + " more bytes than available " + viewSize);
      }
    }
  }
}

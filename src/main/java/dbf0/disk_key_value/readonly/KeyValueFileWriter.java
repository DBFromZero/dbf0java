package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.PrefixIo;

import java.io.*;

public class KeyValueFileWriter implements Closeable {

  private transient OutputStream outputStream;

  KeyValueFileWriter(OutputStream outputStream) {
    Preconditions.checkNotNull(outputStream);
    this.outputStream = outputStream instanceof BufferedOutputStream ? outputStream :
        new BufferedOutputStream(outputStream);
  }

  KeyValueFileWriter(String path) throws IOException {
    this(new BufferedOutputStream(new FileOutputStream(path), 0x8000));
  }

  public void store(ByteArrayWrapper key, ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(outputStream != null, "already closed");
    PrefixIo.writeBytes(outputStream, key);
    PrefixIo.writeBytes(outputStream, value);
  }

  @Override public void close() throws IOException {
    if (outputStream != null) {
      outputStream.close();
      outputStream = null;
    }
  }
}

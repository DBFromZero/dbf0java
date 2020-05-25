package dbf0.disk_key_value.readonly.base;

import com.google.common.base.Preconditions;
import dbf0.common.io.Deserializer;
import dbf0.common.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public abstract class BaseKeyValueFileReader<K, V> implements Closeable {

  public static final int DEFAULT_BUFFER_SIZE = 0x8000;

  protected final Deserializer<K> keyDeserializer;
  protected final Deserializer<V> valueDeserializer;

  protected transient InputStream inputStream;

  protected BaseKeyValueFileReader(Deserializer<K> keyDeserializer,
                                   Deserializer<V> valueDeserializer,
                                   InputStream inputStream) {
    this.keyDeserializer = Preconditions.checkNotNull(keyDeserializer);
    this.valueDeserializer = Preconditions.checkNotNull(valueDeserializer);
    this.inputStream = Preconditions.checkNotNull(inputStream);
  }

  public void skipBytes(long bytes) throws IOException {
    IOUtil.skip(inputStream, bytes);
  }

  @Override public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
  }
}

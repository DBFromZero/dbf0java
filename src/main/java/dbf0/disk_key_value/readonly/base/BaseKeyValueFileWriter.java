package dbf0.disk_key_value.readonly.base;

import com.google.common.base.Preconditions;
import dbf0.common.io.Serializer;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public abstract class BaseKeyValueFileWriter<K, V> implements Closeable {

  public static final int DEFAULT_BUFFER_SIZE = 0x8000;

  protected final Serializer<K> keySerializer;
  protected final Serializer<V> valueSerializer;

  protected transient OutputStream outputStream;

  public BaseKeyValueFileWriter(Serializer<K> keySerializer, Serializer<V> valueSerializer, OutputStream outputStream) {
    Preconditions.checkNotNull(outputStream);
    this.outputStream = outputStream instanceof BufferedOutputStream ? outputStream :
        new BufferedOutputStream(outputStream, DEFAULT_BUFFER_SIZE);
    this.keySerializer = Preconditions.checkNotNull(keySerializer);
    this.valueSerializer = Preconditions.checkNotNull(valueSerializer);
  }

  @Override public void close() throws IOException {
    if (outputStream != null) {
      outputStream.close();
      outputStream = null;
    }
  }
}

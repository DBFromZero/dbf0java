package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.ByteArraySerializer;
import dbf0.common.io.Serializer;

import java.io.*;

public class KeyValueFileWriter<K, V> implements Closeable {

  private static final int DEFAULT_BUFFER_SIZE = 0x8000;

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  private transient OutputStream outputStream;

  public KeyValueFileWriter(Serializer<K> keySerializer, Serializer<V> valueSerializer, OutputStream outputStream) {
    Preconditions.checkNotNull(outputStream);
    this.outputStream = outputStream instanceof BufferedOutputStream ? outputStream :
        new BufferedOutputStream(outputStream, DEFAULT_BUFFER_SIZE);
    this.keySerializer = Preconditions.checkNotNull(keySerializer);
    this.valueSerializer = Preconditions.checkNotNull(valueSerializer);
  }

  public static KeyValueFileWriter<ByteArrayWrapper, ByteArrayWrapper> forByteArrays(OutputStream outputStream) {
    return new KeyValueFileWriter<>(ByteArraySerializer.getInstance(), ByteArraySerializer.getInstance(), outputStream);
  }

  public static <K, V> KeyValueFileWriter<K, V> forPath(Serializer<K> keySerializer,
                                                        Serializer<V> valueSerializer,
                                                        String path) throws IOException {
    return new KeyValueFileWriter<>(keySerializer, valueSerializer, new FileOutputStream(path));
  }

  public void append(K key, V value) throws IOException {
    Preconditions.checkState(outputStream != null, "already closed");
    keySerializer.serialize(outputStream, key);
    valueSerializer.serialize(outputStream, value);
  }

  @Override public void close() throws IOException {
    if (outputStream != null) {
      outputStream.close();
      outputStream = null;
    }
  }
}

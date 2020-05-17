package dbf0.disk_key_value.readonly.singlevalue;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.ByteArraySerializer;
import dbf0.common.io.Serializer;
import dbf0.disk_key_value.readonly.base.BaseKeyValueFileWriter;

import java.io.IOException;
import java.io.OutputStream;

public class KeyValueFileWriter<K, V> extends BaseKeyValueFileWriter<K, V> {

  private transient OutputStream outputStream;

  public KeyValueFileWriter(Serializer<K> keySerializer, Serializer<V> valueSerializer, OutputStream outputStream) {
    super(keySerializer, valueSerializer, outputStream);
  }

  public static KeyValueFileWriter<ByteArrayWrapper, ByteArrayWrapper> forByteArrays(OutputStream outputStream) {
    return new KeyValueFileWriter<>(ByteArraySerializer.getInstance(), ByteArraySerializer.getInstance(), outputStream);
  }

  public void append(K key, V value) throws IOException {
    Preconditions.checkState(outputStream != null, "already closed");
    keySerializer.serialize(outputStream, key);
    valueSerializer.serialize(outputStream, value);
  }
}

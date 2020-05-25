package dbf0.disk_key_value.readonly.singlevalue;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.ByteArrayDeserializer;
import dbf0.common.io.Deserializer;
import dbf0.common.io.EndOfStream;
import dbf0.disk_key_value.readonly.base.BaseKeyValueFileReader;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class KeyValueFileReader<K, V> extends BaseKeyValueFileReader<K, V> {

  private boolean haveReadKey = false;

  public KeyValueFileReader(Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer,
                            InputStream inputStream) {
    super(keyDeserializer, valueDeserializer, inputStream);
  }

  public static KeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper> forByteArrays(InputStream inputStream) {
    return bufferStream(ByteArrayDeserializer.getInstance(), ByteArrayDeserializer.getInstance(),
        inputStream);
  }

  public static KeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper> forByteArrays(String path) throws IOException {
    return forByteArrays(new FileInputStream(path));
  }

  public static <K, V> KeyValueFileReader<K, V> bufferStream(Deserializer<K> keyDeserializer,
                                                             Deserializer<V> valueDeserializer,
                                                             InputStream stream) {
    return new KeyValueFileReader<>(keyDeserializer, valueDeserializer,
        new BufferedInputStream(stream, BaseKeyValueFileReader.DEFAULT_BUFFER_SIZE));
  }

  @Nullable public K readKey() throws IOException {
    Preconditions.checkState(inputStream != null, "already closed");
    Preconditions.checkState(!haveReadKey);
    K key;
    try {
      key = keyDeserializer.deserialize(inputStream);
    } catch (EndOfStream ignored) {
      return null;
    }
    haveReadKey = true;
    return key;
  }

  public V readValue() throws IOException {
    Preconditions.checkState(inputStream != null, "already closed");
    Preconditions.checkState(haveReadKey);
    V value = valueDeserializer.deserialize(inputStream);
    haveReadKey = false;
    return value;
  }

  public void skipValue() throws IOException {
    Preconditions.checkState(inputStream != null, "already closed");
    Preconditions.checkState(haveReadKey);
    valueDeserializer.skipDeserialize(inputStream);
    haveReadKey = false;
  }

  @Nullable Pair<K, V> readKeyValue() throws IOException {
    var key = readKey();
    if (key == null) {
      return null;
    }
    return Pair.of(key, readValue());
  }
}

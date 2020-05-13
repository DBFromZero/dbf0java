package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.EndOfStream;
import dbf0.common.io.ByteArrayDeserializer;
import dbf0.common.io.Deserializer;
import dbf0.common.io.IOUtil;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.*;

public class KeyValueFileReader<K, V> implements Closeable {

  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;

  private transient InputStream inputStream;
  private boolean haveReadKey = false;

  public KeyValueFileReader(Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer,
                            InputStream inputStream) {
    this.keyDeserializer = Preconditions.checkNotNull(keyDeserializer);
    this.valueDeserializer = Preconditions.checkNotNull(valueDeserializer);
    this.inputStream = Preconditions.checkNotNull(inputStream);
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
        new BufferedInputStream(stream, 0x4000));
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

  void skipBytes(long bytes) throws IOException {
    IOUtil.skip(inputStream, bytes);
  }

  @Override public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
  }
}

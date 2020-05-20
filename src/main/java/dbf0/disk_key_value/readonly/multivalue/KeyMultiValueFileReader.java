package dbf0.disk_key_value.readonly.multivalue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import dbf0.common.io.Deserializer;
import dbf0.common.io.EndOfStream;
import dbf0.common.io.IOIterator;
import dbf0.common.io.IOUtil;
import dbf0.disk_key_value.readonly.base.BaseKeyValueFileReader;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

public class KeyMultiValueFileReader<K, V> extends BaseKeyValueFileReader<K, V> {

  private int valuesCount = 0;
  private int valuesRemaining = 0;

  private IOIterator<V> valueIterator = null;

  public KeyMultiValueFileReader(Deserializer<K> keyDeserializer,
                                 Deserializer<V> valueDeserializer,
                                 InputStream inputStream) {
    super(keyDeserializer, valueDeserializer, inputStream);
  }

  public static <K, V> KeyMultiValueFileReader<K, V> bufferStream(Deserializer<K> keyDeserializer,
                                                                  Deserializer<V> valueDeserializer,
                                                                  InputStream stream) {
    return new KeyMultiValueFileReader<>(keyDeserializer, valueDeserializer,
        new BufferedInputStream(stream, DEFAULT_BUFFER_SIZE));
  }

  @Nullable public K readKey() throws IOException {
    Preconditions.checkState(inputStream != null, "already closed");
    Preconditions.checkState(valuesRemaining == 0, "there are still values remaining");
    K key;
    try {
      key = keyDeserializer.deserialize(inputStream);
    } catch (EndOfStream ignored) {
      valuesCount = valuesRemaining = 0;
      return null;
    }
    valuesCount = valuesRemaining = IOUtil.readVariableLengthUnsignedInt(inputStream);
    return key;
  }

  public int getValuesCount() {
    return valuesCount;
  }

  public int getValuesRemaining() {
    return valuesRemaining;
  }

  public V readValue() throws IOException {
    Preconditions.checkState(inputStream != null, "already closed");
    Preconditions.checkState(valuesRemaining > 0);
    V value = valueDeserializer.deserialize(inputStream);
    valuesRemaining--;
    return value;
  }

  public void skipValue() throws IOException {
    Preconditions.checkState(inputStream != null, "already closed");
    Preconditions.checkState(valuesRemaining > 0);
    valueDeserializer.skipDeserialize(inputStream);
    valuesRemaining--;
  }

  public void skipRemainingValues() throws IOException {
    Preconditions.checkState(inputStream != null, "already closed");
    for (; valuesRemaining > 0; valuesRemaining--) {
      valueDeserializer.skipDeserialize(inputStream);
    }
  }

  public IOIterator<V> valueIterator() {
    Preconditions.checkState(inputStream != null, "already closed");
    if (valueIterator == null) {
      valueIterator = new IOIterator<V>() {
        @Override public boolean hasNext() {
          return valuesRemaining > 0;
        }

        @Override public V next() throws IOException {
          if (valuesRemaining == 0) {
            throw new NoSuchElementException();
          }
          return readValue();
        }

        @Override public void skip() throws IOException {
          skipValue();
        }

        @Override public void close() throws IOException {
          KeyMultiValueFileReader.this.close();
        }
      };
    }
    return valueIterator;
  }

  @Override public void close() throws IOException {
    super.close();
    valuesCount = valuesRemaining = 0;
    valueIterator = null;
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper("KMVFR")
        .add("valuesCount", valuesCount)
        .add("valuesRemaining", valuesRemaining)
        .add("inputStream", inputStream)
        .toString();
  }
}

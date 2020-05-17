package dbf0.disk_key_value.readonly.multivalue;

import com.google.common.base.Preconditions;
import dbf0.common.io.IOIterator;
import dbf0.common.io.IOUtil;
import dbf0.common.io.Serializer;
import dbf0.disk_key_value.readonly.base.BaseKeyValueFileWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public class KeyMultiValueFileWriter<K, V> extends BaseKeyValueFileWriter<K, V> {

  private int valuesToBeWritten = 0;

  public KeyMultiValueFileWriter(Serializer<K> keySerializer, Serializer<V> valueSerializer, OutputStream outputStream) {
    super(keySerializer, valueSerializer, outputStream);
  }

  public void startKey(K key, int valueCount) throws IOException {
    Preconditions.checkArgument(valueCount > 0, "bad valueCount %s", valueCount);
    Preconditions.checkState(outputStream != null, "already closed");
    Preconditions.checkState(valuesToBeWritten == 0, "still waiting on %s values from last key", valuesToBeWritten);
    keySerializer.serialize(outputStream, key);
    IOUtil.writeVariableLengthUnsignedInt(outputStream, valueCount);
    valuesToBeWritten = valueCount;
  }

  public void writeValue(V value) throws IOException {
    Preconditions.checkState(valuesToBeWritten > 0, "attempt to write too many values for key");
    valueSerializer.serialize(outputStream, value);
    valuesToBeWritten--;
  }

  public void writeKeysAndValues(K key, int valueCount, IOIterator<? extends V> valueIterator) throws IOException {
    startKey(key, valueCount);
    while (valueIterator.hasNext()) {
      writeValue(valueIterator.next());
    }
    Preconditions.checkState(valuesToBeWritten == 0);
  }

  public void writeKeysAndValues(K key, Collection<? extends V> values) throws IOException {
    writeKeysAndValues(key, values.size(), IOIterator.of(values.iterator()));
  }

}

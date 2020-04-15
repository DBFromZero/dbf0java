package dbf0.disk_key_value.readonly;

import com.google.common.annotations.VisibleForTesting;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

class ReadOnlyKeyValueStorage {

  private final String path;
  private final TreeMap<ByteArrayWrapper, Long> index;

  public ReadOnlyKeyValueStorage(String path, TreeMap<ByteArrayWrapper, Long> index) {
    this.path = path;
    this.index = index;
  }

  public static TreeMap<ByteArrayWrapper, Long> readIndex(String path) throws IOException {
    var iterator = new KeyValueFileIterator(new FileInputStream(path));
    var index = new TreeMap<ByteArrayWrapper, Long>();
    Dbf0Util.iteratorStream(iterator).forEach(
        entry -> index.put(entry.getKey(), ByteBuffer.wrap(entry.getValue().getArray()).getLong()));
    return index;
  }

  @Nullable
  ByteArrayWrapper get(ByteArrayWrapper key) throws IOException {
    return getForReader(key, new KeyValueFileReader(new FileInputStream(path)));
  }

  @VisibleForTesting
  @Nullable
  ByteArrayWrapper getForReader(ByteArrayWrapper key, KeyValueFileReader reader) throws IOException {
    long startIndex = Optional.ofNullable(index.floorEntry(key)).map(Map.Entry::getValue).orElse(0L);
    reader.skipBytes(startIndex);
    return scanForKey(key, reader);
  }

  @VisibleForTesting
  @Nullable
  ByteArrayWrapper scanForKey(ByteArrayWrapper key, KeyValueFileReader reader) throws IOException {
    while (true) {
      var entryKey = reader.readKey();
      if (entryKey == null) {
        return null;
      }
      int cmp = entryKey.compareTo(key);
      if (cmp == 0) {
        return reader.readValue();
      } else if (cmp < 0) {
        reader.skipValue();
      } else {
        return null;
      }
    }
  }
}

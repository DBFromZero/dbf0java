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

class RandomAccessKeyValueFileReader {

  private final String path;
  private final TreeMap<ByteArrayWrapper, Long> index;

  RandomAccessKeyValueFileReader(String path, TreeMap<ByteArrayWrapper, Long> index) {
    this.path = path;
    this.index = index;
  }

  static TreeMap<ByteArrayWrapper, Long> readIndex(String path) throws IOException {
    var iterator = new KeyValueFileIterator(new KeyValueFileReader(path));
    var index = new TreeMap<ByteArrayWrapper, Long>();
    Dbf0Util.iteratorStream(iterator).forEach(
        entry -> index.put(entry.getKey(), ByteBuffer.wrap(entry.getValue().getArray()).getLong()));
    return index;
  }

  @VisibleForTesting
  @Nullable
  ByteArrayWrapper getForReader(ByteArrayWrapper key, KeyValueFileReader reader) throws IOException {
    reader.skipBytes(computeSearchStartIndex(key));
    return scanForKey(key, reader);
  }

  @VisibleForTesting
  long computeSearchStartIndex(ByteArrayWrapper key) {
    return Optional.ofNullable(index.floorEntry(key)).map(Map.Entry::getValue).orElse(0L);
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

  @Nullable
  ByteArrayWrapper get(ByteArrayWrapper key) throws IOException {
    // specifically don't use buffered IO cause we hopefully won't have to read much
    // and we also get to use KeyValueFileReader.skipValue to avoid large segments of data
    // might be worth benchmarking the effect of buffered IO?
    try (var reader = new KeyValueFileReader(new FileInputStream(path))) {
      return getForReader(key, reader);
    }
  }
}

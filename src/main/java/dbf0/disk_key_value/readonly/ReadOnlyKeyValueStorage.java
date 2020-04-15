package dbf0.disk_key_value.readonly;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.EndOfStream;
import dbf0.common.PrefixIo;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class ReadOnlyKeyValueStorage {

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

  public Optional<ByteArrayWrapper> get(ByteArrayWrapper key) throws IOException {
    return getForStream(key, new FileInputStream(path));
  }

  @VisibleForTesting
  Optional<ByteArrayWrapper> getForStream(ByteArrayWrapper key, FileInputStream stream) throws IOException {
    long startIndex = Optional.ofNullable(index.floorEntry(key)).map(Map.Entry::getValue).orElse(0L);
    long skipped = stream.skip(startIndex);
    Preconditions.checkState(skipped == startIndex);
    return scanForKey(key, stream);
  }

  @VisibleForTesting
  Optional<ByteArrayWrapper> scanForKey(ByteArrayWrapper key, FileInputStream stream) throws IOException {
    while (true) {
      int recordTotalLength;
      try {
        recordTotalLength = PrefixIo.readLength(stream);
      } catch (EndOfStream ignored) {
        return Optional.empty();
      }
      var entryKey = PrefixIo.readBytes(stream);
      int valueLength = recordTotalLength - entryKey.length();
      int cmp = entryKey.compareTo(key);
      if (cmp == 0) {
        var bytes = new byte[valueLength];
        Dbf0Util.readArrayFully(stream, bytes);
        return Optional.of(ByteArrayWrapper.of(bytes));
      } else if (cmp < 0) {
        long skipped = stream.skip(valueLength);
        Preconditions.checkState(skipped == valueLength);
      } else {
        return Optional.empty();
      }
    }
  }
}

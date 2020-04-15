package dbf0.disk_key_value.readonly;

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
    var index = new TreeMap<ByteArrayWrapper, Long>(ByteArrayWrapper.comparator());
    Dbf0Util.iteratorStream(iterator).forEach(
        entry -> index.put(entry.getKey(), ByteBuffer.wrap(entry.getValue().getArray()).getLong()));
    return index;
  }

  public Optional<ByteArrayWrapper> get(ByteArrayWrapper key) throws IOException {
    long startIndex = Optional.ofNullable(index.floorEntry(key)).map(Map.Entry::getValue).orElse(0L);
    var stream = new FileInputStream(path);
    long skipped = stream.skip(startIndex);
    Preconditions.checkState(skipped == startIndex);
    var comparator = ByteArrayWrapper.comparator();
    while (true) {
      int totalLength;
      try {
        totalLength = PrefixIo.readLength(stream);
      } catch (EndOfStream ignored) {
        return Optional.empty();
      }
      var entryKey = PrefixIo.readBytes(stream);
      int valueLength = totalLength - entryKey.length();
      int cmp = comparator.compare(entryKey, key);
      if (cmp == 0) {
        var bytes = new byte[valueLength];
        Dbf0Util.readArrayFully(stream, bytes);
        return Optional.of(ByteArrayWrapper.of(bytes));
      } else if (cmp < 0) {
        skipped = stream.skip(valueLength);
        Preconditions.checkState(skipped == valueLength);
      } else {
        return Optional.empty();
      }
    }
  }
}

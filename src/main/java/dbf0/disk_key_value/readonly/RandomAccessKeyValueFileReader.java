package dbf0.disk_key_value.readonly;

import com.google.common.annotations.VisibleForTesting;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.ReadOnlyFileOperations;
import dbf0.disk_key_value.io.ReadOnlyFileOperationsImpl;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class RandomAccessKeyValueFileReader {

  private final ReadOnlyFileOperations fileOperations;
  private final TreeMap<ByteArrayWrapper, Long> index;

  public RandomAccessKeyValueFileReader(ReadOnlyFileOperations fileOperations, TreeMap<ByteArrayWrapper, Long> index) {
    this.fileOperations = fileOperations;
    this.index = index;
  }

  RandomAccessKeyValueFileReader(String path, TreeMap<ByteArrayWrapper, Long> index) {
    this(new ReadOnlyFileOperationsImpl(new File(path)), index);
  }

  public static TreeMap<ByteArrayWrapper, Long> readIndex(String path) throws IOException {
    return readIndex(new KeyValueFileIterator(new KeyValueFileReader(path)));
  }

  public static TreeMap<ByteArrayWrapper, Long> readIndex(KeyValueFileIterator iterator) throws IOException {
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
  public ByteArrayWrapper get(ByteArrayWrapper key) throws IOException {
    // specifically don't use buffered IO cause we hopefully won't have to read much
    // and we also get to use KeyValueFileReader.skipValue to avoid large segments of data
    // might be worth benchmarking the effect of buffered IO?
    try (var reader = new KeyValueFileReader(fileOperations.createInputStream())) {
      return getForReader(key, reader);
    }
  }
}

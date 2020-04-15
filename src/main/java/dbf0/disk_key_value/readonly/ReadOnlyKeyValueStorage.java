package dbf0.disk_key_value.readonly;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.annotations.VisibleForTesting;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoConsumer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static dbf0.disk_key_value.readonly.WriteSortedKeyValueFiles.DIRECTORY;
import static dbf0.disk_key_value.readonly.WriteSortedKeyValueFiles.KEY_LENGTH;

public class ReadOnlyKeyValueStorage {

  private final String path;
  private final TreeMap<ByteArrayWrapper, Long> index;

  ReadOnlyKeyValueStorage(String path, TreeMap<ByteArrayWrapper, Long> index) {
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

  public static void main(String[] args) throws Exception {
    var store = new ReadOnlyKeyValueStorage(DIRECTORY + "/merged", readIndex(DIRECTORY + "/index"));
    var iterator = new KeyValueFileIterator(new KeyValueFileReader(DIRECTORY + "/0"));
    var random = new Random(0xCAFE);
    StreamUtils.zipWithIndex(Dbf0Util.iteratorStream(iterator)).forEach(entry -> {
      if (entry.getIndex() % 1000 == 0) {
        System.out.println("Read " + entry.getIndex());
      }
      if (random.nextFloat() > 0.99) {
        var key = entry.getValue().getKey();
        var value = entry.getValue().getValue();
        ByteArrayWrapper readValue;
        try {
          readValue = store.get(key);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (readValue == null) {
          System.out.println("Missing value for " + key);
        } else if (!readValue.equals(value)) {
          System.out.println("Incorrect value for " + key + " got " + readValue + " expected " + value);
          throw new RuntimeException("failed");
        }
      }
    });

    IntStream.range(0, 100).boxed().forEach(IoConsumer.wrap(i -> {
      var bytes = new byte[KEY_LENGTH];
      random.nextBytes(bytes);
      var key = ByteArrayWrapper.of(bytes);
      var value = store.get(key);
      if (value != null) {
        System.out.println("Unexpected value for random key: " + key);
      }
    }));
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
    try (var reader = new KeyValueFileReader(path)) {
      return getForReader(key, reader);
    }
  }
}

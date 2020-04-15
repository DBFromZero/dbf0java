package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import dbf0.common.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

public class NaiveDiskKeyValueStorage implements KeyValueStorage {

  private final String path;
  private transient FileOutputStream stream = null;

  public NaiveDiskKeyValueStorage(String path) {
    this.path = Preconditions.checkNotNull(path);
  }

  public static void main(String[] args) throws Exception {
    var path = "/data/tmp/naive_kv";
    var storage = new NaiveDiskKeyValueStorage(path);
    storage.initialize();
    var random = new Random(0xCAFE);
    Function<Integer, ByteArrayWrapper> randomBw = (length) -> {
      var bytes = new byte[length];
      random.nextBytes(bytes);
      return ByteArrayWrapper.of(bytes);
    };
    var keyLength = 8;
    var valueLength = 2048;
    Map<ByteArrayWrapper, ByteArrayWrapper> stored = new HashMap<>();
    IntStream.range(0, 50 * 1000).boxed().forEach(IoConsumer.wrap(i -> {
      if (i % 1000 == 0) {
        System.out.println("Writing " + i);
      }
      var key = randomBw.apply(keyLength);
      var value = randomBw.apply(valueLength);
      storage.store(key, value);
      if (random.nextFloat() > 0.99) {
        stored.put(key, value);
      }
    }));

    stored.forEach((key, value) -> {
      try {
        System.out.println("Checking: " + key);
        var storeValue = storage.get(key);
        if (storeValue.isEmpty()) {
          throw new RuntimeException("Missing key " + key);
        }
        if (!storeValue.get().equals(value)) {
          throw new RuntimeException("Values not equal " + value + " expected " + storeValue);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public synchronized void initialize() throws IOException {
    Preconditions.checkState(stream == null, "already initialized");
    stream = new FileOutputStream(path, true);
  }

  @Override
  public synchronized void close() throws IOException {
    Preconditions.checkState(stream != null, "not initialized");
    stream.close();
  }

  @Override
  public synchronized void store(ByteArrayWrapper key, ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(stream != null, "not initialized");
    int totalLength = key.length() + value.length();
    PrefixIo.writeLength(stream, totalLength);
    PrefixIo.writeBytes(stream, key);
    stream.write(value.getArray());
  }

  @Override
  public Optional<ByteArrayWrapper> get(ByteArrayWrapper key) throws IOException {
    var input = new FileInputStream(path);
    while (true) {
      int totalLength;
      try {
        totalLength = PrefixIo.readLength(input);
      } catch (EndOfStream ignored) {
        break;
      }
      var entryKey = PrefixIo.readBytes(input);
      int valueLength = totalLength - entryKey.length();
      if (entryKey.equals(key)) {
        var bytes = new byte[valueLength];
        Dbf0Util.readArrayFully(input, bytes);
        return Optional.of(ByteArrayWrapper.of(bytes));
      } else {
        long skipped = input.skip(valueLength);
        if (skipped != valueLength) {
          break;
        }
      }
    }
    return Optional.empty();
  }
}

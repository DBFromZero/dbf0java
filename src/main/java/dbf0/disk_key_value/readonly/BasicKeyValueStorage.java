package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.IoConsumer;
import dbf0.common.PrefixIo;

import javax.annotation.Nullable;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

public class BasicKeyValueStorage {

  interface StreamFactory {
    OutputStream out() throws IOException;

    FileInputStream in() throws IOException;
  }

  static StreamFactory pathStreamFactory(String path) {
    Preconditions.checkNotNull(path);
    return new StreamFactory() {
      @Override
      public BufferedOutputStream out() throws IOException {
        return new BufferedOutputStream(new FileOutputStream(path), 0x8000);
      }

      @Override
      public FileInputStream in() throws IOException {
        return new FileInputStream(path);
      }
    };
  }

  private final StreamFactory streamFactory;
  private transient OutputStream outputStream = null;

  BasicKeyValueStorage(StreamFactory streamFactory) {
    this.streamFactory = Preconditions.checkNotNull(streamFactory);
  }

  BasicKeyValueStorage(String path) {
    this.streamFactory = pathStreamFactory(path);
  }


  synchronized void initialize() throws IOException {
    Preconditions.checkState(outputStream == null, "already initialized");
    outputStream = streamFactory.out();
  }

  synchronized void close() throws IOException {
    Preconditions.checkState(outputStream != null, "not initialized");
    outputStream.close();
  }

  synchronized void store(ByteArrayWrapper key, ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(outputStream != null, "not initialized");
    PrefixIo.writeBytes(outputStream, key);
    PrefixIo.writeBytes(outputStream, value);
  }

  @Nullable
  ByteArrayWrapper get(ByteArrayWrapper key) throws IOException {
    var reader = new KeyValueFileReader(streamFactory.in());
    while (true) {
      var entryKey = reader.readKey();
      if (entryKey == null) {
        return null;
      }
      if (entryKey.equals(key)) {
        return reader.readValue();
      } else {
        reader.skipValue();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    var path = "/data/tmp/naive_kv";
    var storage = new BasicKeyValueStorage(pathStreamFactory(path));
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
      if (random.nextFloat() > 0.999) {
        stored.put(key, value);
      }
    }));

    stored.forEach((key, value) -> {
      try {
        System.out.println("Checking: " + key);
        var storeValue = storage.get(key);
        if (storeValue == null) {
          throw new RuntimeException("Missing key " + key);
        }
        if (!storeValue.equals(value)) {
          throw new RuntimeException("Values not equal " + value + " expected " + storeValue);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
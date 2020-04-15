package dbf0.disk_key_value.readonly;

import com.codepoetics.protonpack.StreamUtils;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.IoConsumer;

import java.io.IOException;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

public class WriteSortedKeyValueFiles {

  public static final int FILES = 20;
  public static final int KEY_LENGTH = 16;
  public static final int VALUE_LENGTH = 4096;
  public static final int FILE_ENTRIES = 100 * 1000;
  public static final String DIRECTORY = "/data/tmp/sorted_kv_files";

  private static void writeFile(String path, long seed) throws IOException {
    var random = new Random(seed);
    Function<Integer, ByteArrayWrapper> randomBw = (length) -> {
      var bytes = new byte[length];
      random.nextBytes(bytes);
      return ByteArrayWrapper.of(bytes);
    };
    System.out.println("Generating sorted keys for" + path);
    var sortedKeys = IntStream.range(0, FILE_ENTRIES)
        .mapToObj(ignored -> randomBw.apply(KEY_LENGTH))
        .sorted();

    var storage = new BasicKeyValueStorage(path);
    storage.initialize();

    StreamUtils.zipWithIndex(sortedKeys).forEach(indexed -> {
      if (indexed.getIndex() % 10000 == 0) {
        System.out.println("Writing entry " + indexed.getIndex() + " of " + path);
      }
      var value = randomBw.apply(VALUE_LENGTH);
      try {
        storage.store(indexed.getValue(), value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    storage.close();
  }

  public static void main(String[] args) {
    var random = new Random(0xCAFE);
    IntStream.range(0, FILES).boxed().parallel().forEach(IoConsumer.wrap(index ->
        writeFile(DIRECTORY + "/" + index, random.nextLong())
    ));
  }
}

package dbf0.disk_key_value.readonly;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoConsumer;

import java.io.IOException;
import java.util.Random;
import java.util.stream.IntStream;

public class WriteSortedKeyValueFiles {

  public static final int KEY_LENGTH = 16;

  private static void writeFile(String path, int valueLength, int entriesCount) throws IOException {
    var random = new Random();
    System.out.println("Generating sorted keys for" + path);
    var sortedKeys = IntStream.range(0, entriesCount)
        .mapToObj(ignored -> ByteArrayWrapper.random(random, KEY_LENGTH))
        .sorted();

    var storage = new BasicKeyValueStorage(path);
    storage.initialize();

    StreamUtils.zipWithIndex(sortedKeys).forEach(IoConsumer.wrap(indexed -> {
      if (indexed.getIndex() % 10000 == 0) {
        System.out.println("Writing entry " + indexed.getIndex() + " of " + path);
      }
      storage.store(indexed.getValue(), ByteArrayWrapper.random(random, valueLength));
    }));
    storage.close();
  }

  public static void main(String[] args) {
    Preconditions.checkArgument(args.length == 4);
    var directory = args[0];
    var fileCount = Integer.parseInt(args[1]);
    var valueLength = Integer.parseInt(args[2]);
    var entriesCount = Math.round(Float.parseFloat(args[3]));

    Dbf0Util.requireEmptyDirectory(directory);
    IntStream.range(0, fileCount).boxed().parallel().forEach(IoConsumer.wrap(index ->
        writeFile(directory + "/" + index, valueLength, entriesCount)
    ));
  }
}

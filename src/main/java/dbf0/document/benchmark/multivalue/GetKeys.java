package dbf0.document.benchmark.multivalue;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.io.IOConsumer;
import dbf0.common.io.IOSupplier;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.ValueWrapper;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.serialization.DElementSerializer;
import dbf0.document.types.DElement;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetKeys {
  private static final Logger LOGGER = Dbf0Util.getLogger(GetKeys.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var directory = new File(argsItr.next());
    var outputPath = new File(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());
    Preconditions.checkState(directory.isDirectory());

    var executor = Executors.newWorkStealingPool();
    try {
      var keysStream = getKeys(directory, executor);
      try (var stream = new BufferedOutputStream(new FileOutputStream(outputPath), 0x8000)) {
        var serializer = DElementSerializer.defaultCharsetInstance();
        keysStream.forEach(IOConsumer.wrap(key -> serializer.serialize(stream, key)));
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  public static boolean isPartitionFileName(String name) {
    return name.matches("^\\d+$");
  }

  public static boolean isPartitionFile(File file) {
    return isPartitionFileName(file.getName());
  }

  public static boolean isLsmTreeDataFileName(String name) {
    return name.matches("^(base|(?:delta-\\d+))$");
  }

  public static boolean isLsmTreeDataFile(File file) {
    return isLsmTreeDataFileName(file.getName());
  }

  private static Stream<DElement> getKeys(File directory, ExecutorService executor) throws Exception {
    var files = Arrays.stream(directory.listFiles())
        .filter(File::isDirectory)
        .filter(GetKeys::isPartitionFile)
        .map(File::listFiles)
        .flatMap(Arrays::stream)
        .filter(File::isFile)
        .filter(GetKeys::isLsmTreeDataFile)
        .collect(Collectors.toList());
    LOGGER.info("Found " + files.size() + " data files");
    var futures = files.stream()
        .map(f -> CompletableFuture.supplyAsync(IOSupplier.wrap(() -> loadPartition(f)), executor))
        .collect(Collectors.toList());
    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get();
    return futures.stream().map(Dbf0Util::getUnchecked).flatMap(Collection::stream);
  }

  private static Collection<DElement> loadPartition(File file) throws IOException {
    LOGGER.info("Loading " + file.getPath());
    try (var reader = KeyMultiValueFileReader.bufferStream(
        DElementDeserializer.defaultCharsetInstance(),
        ValueWrapper.serializationPair(DElement.sizePrefixedSerializationPair())
            .getDeserializer(),
        new FileInputStream(file))) {
      var keys = new ArrayList<DElement>(Dbf0Util.safeLongToInt(file.length() / 1024L));
      while (true) {
        var key = reader.readKey();
        if (key == null) {
          break;
        }
        keys.add(key);
        reader.skipRemainingValues();
      }
      LOGGER.info("Loaded " + keys.size() + " from " + file.getPath());
      return keys;
    }
  }
}

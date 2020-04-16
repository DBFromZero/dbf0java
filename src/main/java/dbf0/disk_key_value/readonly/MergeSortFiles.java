package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeSortFiles {

  private static final Logger LOGGER = Dbf0Util.getLogger(MergeSortFiles.class);

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length >= 3);
    Dbf0Util.enableConsoleLogging(Level.INFO);

    var sortedFilesDirectory = args[0];
    var sortedFilesCount = Integer.parseInt(args[1]);
    var outputFilePath = args[2];
    var indexSpecs = Arrays.stream(args).skip(3).map(MergeSortFiles::parseIndexSpec).collect(Collectors.toList());

    LOGGER.info("Reading " + sortedFilesCount + " sorted files from " + sortedFilesDirectory);
    LOGGER.info(" Writing merged output to " + outputFilePath);
    LOGGER.info("Writing " + indexSpecs.size() + " indices");
    indexSpecs.forEach(spec -> LOGGER.info("  Index rate " + spec.getRight() + " in " + spec.getLeft()));

    var iterators = IntStream.range(0, sortedFilesCount).boxed().map(IoFunction.wrap(index ->
        new KeyValueFileIterator(new KeyValueFileReader(sortedFilesDirectory + "/" + index))))
        .collect(Collectors.toList());
    var sortedIterator = Iterators.mergeSorted(iterators, Map.Entry.comparingByKey());

    var indexBuilder = IndexBuilder.multiIndexBuilder(
        indexSpecs.stream().map(MergeSortFiles::createIndexBuilder).collect(Collectors.toList()));

    var outputStream = new PositionTrackingStream(outputFilePath);
    var storage = new BasicKeyValueStorage(new BasicKeyValueStorage.StreamFactory() {
      @Override
      public FileOutputStream out() {
        return outputStream;
      }

      @Override
      public FileInputStream in() {
        throw new RuntimeException("input not supported");
      }
    });
    storage.initialize();

    ByteArrayWrapper lastKey = null;
    int i = 0;
    while (sortedIterator.hasNext()) {
      var entry = sortedIterator.next();
      if (i % 10000 == 0) {
        LOGGER.info("Writing merged " + i);
      }
      i++;
      if (lastKey != null && lastKey.equals(entry.getKey())) {
        LOGGER.info("Skipping duplicate key " + lastKey);
        continue;
      }
      lastKey = entry.getKey();
      indexBuilder.accept(outputStream.position, entry.getKey());
      storage.store(entry.getKey(), entry.getValue());
    }
    storage.close();
  }

  private static Pair<String, Integer> parseIndexSpec(String spec) {
    var parts = spec.split(Pattern.quote(","));
    Preconditions.checkArgument(parts.length == 2);
    int rate = Integer.parseInt(parts[1]);
    Preconditions.checkArgument(rate > 0);
    return Pair.of(parts[0], rate);
  }

  private static IndexBuilder createIndexBuilder(Pair<String, Integer> spec) {
    try {
      var storage = new BasicKeyValueStorage(spec.getLeft());
      var builder = IndexBuilder.indexBuilder(storage, spec.getRight());
      storage.initialize();
      return builder;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class PositionTrackingStream extends FileOutputStream {

    private long position = 0;

    public PositionTrackingStream(String name) throws FileNotFoundException {
      super(name);
    }

    @Override
    public void write(int b) throws IOException {
      super.write(b);
      position += 1;
    }

    @Override
    public void write(@NotNull byte[] b) throws IOException {
      super.write(b);
      position += b.length;
    }

    @Override
    public void write(@NotNull byte[] b, int off, int len) throws IOException {
      super.write(b, off, len);
      position += len;
    }
  }
}

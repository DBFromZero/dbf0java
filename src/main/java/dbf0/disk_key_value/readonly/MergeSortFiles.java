package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoFunction;
import dbf0.common.PositionTrackingStream;
import org.apache.commons.lang3.tuple.Pair;

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
    Dbf0Util.enableConsoleLogging(Level.FINE);

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
    @SuppressWarnings("UnstableApiUsage") var sortedIterator = Iterators.mergeSorted(iterators, Map.Entry.comparingByKey());

    var indexBuilder = IndexBuilder.multiIndexBuilder(
        indexSpecs.stream().map(MergeSortFiles::createIndexBuilder).collect(Collectors.toList()));

    try (var outputStream = new PositionTrackingStream(outputFilePath)) {
      try (var storage = new KeyValueFileWriter(outputStream)) {
        ByteArrayWrapper lastKey = null;
        int i = 0;
        while (sortedIterator.hasNext()) {
          var entry = sortedIterator.next();
          if (i % 10000 == 0) {
            LOGGER.fine("Writing merged " + i);
          }
          i++;
          if (lastKey != null && lastKey.equals(entry.getKey())) {
            LOGGER.warning("Skipping duplicate key " + lastKey);
            continue;
          }
          lastKey = entry.getKey();
          indexBuilder.accept(outputStream.getPosition(), entry.getKey());
          storage.append(entry.getKey(), entry.getValue());
        }
      }
    }
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
      return IndexBuilder.indexBuilder(new KeyValueFileWriter(spec.getLeft()), spec.getRight());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

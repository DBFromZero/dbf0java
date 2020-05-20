package dbf0.document.benchmark.multivalue;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static dbf0.document.benchmark.multivalue.BenchmarkLoadMultiValueDocuments.createLsmTree;

public class MergedPartitionedMultiValueLsmTreeDeltas {

  private static final Logger LOGGER = Dbf0Util.getLogger(MergedPartitionedMultiValueLsmTreeDeltas.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);

    var argsItr = Arrays.asList(args).iterator();
    var directory = new File(argsItr.next());
    var partitions = Integer.parseInt(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());
    Preconditions.checkState(directory.isDirectory());

    var base = new FileDirectoryOperationsImpl(directory);
    var executor = Executors.newScheduledThreadPool(4);
    try {
      for (int partition = 0; partition < partitions; partition++) {
        var partitionDir = base.subDirectory(String.valueOf(partition));
        LOGGER.info("merging " + partitionDir);
        Preconditions.checkState(partitionDir.exists());
        try (var tree = createLsmTree(partitionDir, 10000,
            indexRate, executor)) {
          tree.initialize();
          tree.waitForAllDeltasToMerge();
        }
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}

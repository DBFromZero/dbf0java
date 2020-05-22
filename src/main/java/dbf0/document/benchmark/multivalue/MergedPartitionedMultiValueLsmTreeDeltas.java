package dbf0.document.benchmark.multivalue;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import dbf0.common.Dbf0Util;
import dbf0.common.io.IOFunction;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.MultiValueLsmTree;
import dbf0.document.types.DElement;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dbf0.document.benchmark.multivalue.BenchmarkLoadMultiValueDocuments.createLsmTree;

public class MergedPartitionedMultiValueLsmTreeDeltas {

  private static final Logger LOGGER = Dbf0Util.getLogger(MergedPartitionedMultiValueLsmTreeDeltas.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);

    var lsmTreeExecutor = Executors.newScheduledThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors() - 4));
    try {
      var lsmTrees = Arrays.stream(args).flatMap(IOFunction.wrap(root -> openLsmTrees(new File(root), lsmTreeExecutor)));
      var error = new AtomicBoolean(false);
      var waitingPool = Executors.newFixedThreadPool(4);
      try {
        var futures = lsmTrees.map(pair ->
            (Future<Void>) waitingPool.submit(() -> run(error, pair.getLeft(), pair.getRight())))
            .collect(Collectors.toCollection(LinkedList::new));
        while (!error.get() && !futures.isEmpty()) {
          var future = futures.removeFirst();
          try {
            future.get(500, TimeUnit.MILLISECONDS);
          } catch (TimeoutException ignored) {
            futures.add(future);
          }
        }
        if (error.get()) {
          for (var future : futures) {
            future.cancel(true);
          }
        }
      } finally {
        waitingPool.shutdownNow();
        waitingPool.awaitTermination(10, TimeUnit.SECONDS);
      }
    } finally {
      lsmTreeExecutor.shutdown();
      lsmTreeExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @NotNull static Stream<Pair<File, MultiValueLsmTree<FileOutputStream, DElement, DElement>>>
  openLsmTrees(File root, ScheduledExecutorService executor) throws IOException {
    var config = readConfig(new File(root, "config.json"));
    int pendingWritesMergeThreshold = config.getLeft();
    int indexRate = config.getRight();
    var partitionDir = new File(root, "partitions");
    Preconditions.checkState(partitionDir.isDirectory());
    return Arrays.stream(partitionDir.listFiles()).filter(GetKeys::isPartitionFile)
        .map(d -> Pair.of(d, createLsmTree(new FileDirectoryOperationsImpl(d), pendingWritesMergeThreshold, indexRate, executor)));
  }

  @NotNull static Pair<Integer, Integer> readConfig(File file) throws IOException {
    JsonObject jsonConfig;
    try (var fileReader = new FileReader(file)) {
      jsonConfig = new Gson().fromJson(fileReader, JsonObject.class);
    }
    Preconditions.checkNotNull(jsonConfig);
    return Pair.of(
        jsonConfig.getAsJsonPrimitive("pendingWritesMergeThreshold").getAsInt(),
        jsonConfig.getAsJsonPrimitive("indexRate").getAsInt()
    );
  }

  private static void run(AtomicBoolean error, File file, MultiValueLsmTree<FileOutputStream, DElement, DElement> tree) {
    if (error.get()) {
      return;
    }
    LOGGER.info("Merging " + file);
    try (tree) {
      tree.initialize();
      tree.waitForAllDeltasToMerge();
    } catch (InterruptedException ignored) {
    } catch (Throwable t) {
      LOGGER.log(Level.SEVERE, "error in merging " + file);
      error.set(true);
    }
  }
}

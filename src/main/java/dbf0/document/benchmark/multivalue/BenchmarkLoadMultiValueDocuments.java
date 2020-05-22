package dbf0.document.benchmark.multivalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import dbf0.common.Dbf0Util;
import dbf0.common.ParallelThreads;
import dbf0.common.io.EndOfStream;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.HashPartitionedMultiValueReadWriteStorage;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.MultiValueLsmTree;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.ValueWrapper;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.types.DElement;
import dbf0.document.types.DMap;
import dbf0.document.types.DString;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static dbf0.document.benchmark.singlevalue.BenchmarkLoadDocuments.fileSize;

public class BenchmarkLoadMultiValueDocuments {
  private static final Logger LOGGER = Dbf0Util.getLogger(BenchmarkLoadMultiValueDocuments.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var readFile = new File(argsItr.next());
    var directory = new File(argsItr.next());
    var idKey = argsItr.next();
    var valueKey = argsItr.next();
    var pendingWritesMergeThreshold = Integer.parseInt(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    var partitions = Integer.parseInt(argsItr.next());
    var writingThreadsCount = Integer.parseInt(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());

    var base = new FileDirectoryOperationsImpl(directory);
    base.mkdirs();
    base.clear();

    var error = new AtomicBoolean(false);
    var readDone = new AtomicBoolean(false);
    var writes = new AtomicLong(0);
    long startTime;

    var executor = Executors.newScheduledThreadPool(
        Math.max(2, Runtime.getRuntime().availableProcessors() - writingThreadsCount - 2));
    try (var store = createNewStoreWithBackgroundTasks(pendingWritesMergeThreshold, indexRate, partitions, base, executor)) {
      store.initialize();

      startTime = System.nanoTime();
      var reportFuture = executor.scheduleWithFixedDelay(() -> report(error, writes, directory, startTime),
          1, 1, TimeUnit.SECONDS);

      var queue = new LinkedBlockingQueue<DMap>(1000);

      try (var readThread = new ParallelThreads(error, List.of(new Thread(() -> read(error, queue, readFile))))) {
        readThread.start();

        try (var writeThreads = ParallelThreads.create(error, writingThreadsCount,
            i -> new Thread(() -> write(error, readDone, writes, idKey, valueKey, queue, store),
                "write-" + i))) {
          writeThreads.start();

          readThread.awaitCompletion();
          readDone.set(true);
          if (error.get()) {
            readThread.abort();
            writeThreads.abort();
          } else {
            writeThreads.awaitCompletion();
          }
        }
      }
      reportFuture.cancel(false);
    }

    report(error, writes, directory, startTime);
    System.exit(error.get() ? 1 : 0);
  }


  public static void report(AtomicBoolean errors, AtomicLong writes,
                            File directory, long startTime) {
    if (errors.get()) {
      return;
    }
    var time = System.nanoTime();
    var size = fileSize(directory);
    var writesValue = writes.get();
    var stats = ImmutableMap.<String, Object>builder()
        .put("time", time)
        .put("writes", writesValue)
        .put("size", size)
        .build();
    System.out.println(new Gson().toJson(stats));
    LOGGER.info(String.format("%s writes=%.3e size=%s",
        Duration.ofNanos(time - startTime),
        (double) writesValue,
        Dbf0Util.formatBytes(size)));
  }

  @NotNull static MultiValueReadWriteStorageWithBackgroundTasks<DElement, DElement>
  createNewStoreWithBackgroundTasks(int pendingWritesMergeThreshold, int indexRate, int partitions,
                                    FileDirectoryOperationsImpl root, ScheduledExecutorService executor)
      throws IOException {
    Preconditions.checkState(root.exists());
    Preconditions.checkState(root.list().isEmpty());
    var configFile = root.file("config.json");
    try (var writer = new BufferedWriter(new OutputStreamWriter(configFile.createAppendOutputStream()))) {
      var gson = new GsonBuilder().setPrettyPrinting().create();
      var config = new JsonObject();
      config.addProperty("pendingWritesMergeThreshold", pendingWritesMergeThreshold);
      config.addProperty("indexRate", indexRate);
      gson.toJson(config, writer);
    }
    return createStoreWithBackgroundTasks(pendingWritesMergeThreshold, indexRate, partitions,
        root.subDirectory("partitions"), executor);
  }

  @NotNull static MultiValueReadWriteStorageWithBackgroundTasks<DElement, DElement>
  createStoreWithBackgroundTasks(int pendingWritesMergeThreshold, int indexRate, int partitions,
                                 FileDirectoryOperationsImpl stores, ScheduledExecutorService executor) throws IOException {
    return new MultiValueReadWriteStorageWithBackgroundTasks<>(
        createStore(pendingWritesMergeThreshold, indexRate, partitions, stores, executor),
        executor
    );
  }

  @NotNull static HashPartitionedMultiValueReadWriteStorage<DElement, DElement>
  createStore(int pendingWritesMergeThreshold, int indexRate, int partitions,
              FileDirectoryOperationsImpl stores, ScheduledExecutorService executor) throws IOException {
    return HashPartitionedMultiValueReadWriteStorage.create(partitions,
        partition -> createNewLsmTree(stores.subDirectory(String.valueOf(partition)),
            pendingWritesMergeThreshold, indexRate, executor));
  }

  @NotNull static MultiValueLsmTree<FileOutputStream, DElement, DElement>
  createNewLsmTree(FileDirectoryOperationsImpl directoryOperations,
                   int pendingWritesMergeThreshold, int indexRate,
                   ScheduledExecutorService executorService) throws IOException {
    directoryOperations.mkdirs();
    return createLsmTree(directoryOperations, pendingWritesMergeThreshold, indexRate, executorService);
  }

  @NotNull static MultiValueLsmTree<FileOutputStream, DElement, DElement>
  createLsmTree(FileDirectoryOperationsImpl directoryOperations,
                int pendingWritesMergeThreshold, int indexRate, ScheduledExecutorService executorService) {
    return MultiValueLsmTree.<FileOutputStream>builderForDocuments(
        createConfigBuilder(pendingWritesMergeThreshold, indexRate).build())
        .withBaseDeltaFiles(directoryOperations)
        .withScheduledExecutorService(executorService)
        .build();
  }

  @NotNull static LsmTreeConfiguration.Builder<DElement, ValueWrapper<DElement>>
  createConfigBuilder(int pendingWritesMergeThreshold, int indexRate) {
    return LsmTreeConfiguration.builderForMultiValueDocumentsIndex()
        .withPendingWritesDeltaThreshold(pendingWritesMergeThreshold)
        .withIndexRate(indexRate)
        .withMaxInFlightWriteJobs(3)
        .withMaxDeltaReadPercentage(0.75)
        .withMergeCronFrequency(Duration.ofSeconds(1))
        .withMaxDeltasPerMerge(30);
  }

  private static void read(AtomicBoolean error, BlockingQueue<DMap> queue, File file) {
    var deserializer = DElementDeserializer.defaultCharsetInstance();
    try (var stream = new BufferedInputStream(new FileInputStream(file), 0x8000)) {
      while (!error.get()) {
        DElement element;
        try {
          element = deserializer.deserialize(stream);
        } catch (EndOfStream ignored) {
          break;
        }
        queue.put((DMap) element);
      }
    } catch (InterruptedException ignored) {
    } catch (Exception e) {
      error.set(true);
      LOGGER.log(Level.SEVERE, e, () -> "Error in reading");
    }
  }

  private static void write(AtomicBoolean error, AtomicBoolean readDone, AtomicLong writes,
                            String idKey, String valueKey,
                            BlockingQueue<DMap> queue,
                            MultiValueReadWriteStorage<DElement, DElement> store) {
    var dIdKey = DString.of(idKey);
    var dValueKey = valueKey.isEmpty() ? null : DString.of(valueKey);
    try {
      while (!error.get()) {
        var element = queue.poll(100, TimeUnit.MILLISECONDS);
        if (element == null) {
          if (readDone.get()) {
            break;
          }
          continue;
        }
        var id = element.getEntries().get(dIdKey);
        Preconditions.checkState(id != null, "no such id %s in %s", dIdKey, element);
        var value = dValueKey == null ? element : Preconditions.checkNotNull(element.getEntries().get(dValueKey),
            "no such value %s in %s", dValueKey, element);
        store.put(id, value);
        writes.incrementAndGet();
      }
    } catch (InterruptedException ignored) {
    } catch (Exception e) {
      error.set(true);
      LOGGER.log(Level.SEVERE, e, () -> "Error in writing");
    }
  }
}

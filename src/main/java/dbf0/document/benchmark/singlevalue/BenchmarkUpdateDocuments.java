package dbf0.document.benchmark.singlevalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;
import dbf0.document.types.DElement;
import dbf0.document.types.DInt;
import dbf0.document.types.DMap;
import dbf0.document.types.DString;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static dbf0.document.benchmark.singlevalue.BenchmarkGetDocuments.loadKeys;
import static dbf0.document.benchmark.singlevalue.BenchmarkLoadDocuments.fileSize;

public class BenchmarkUpdateDocuments {
  private static final Logger LOGGER = Dbf0Util.getLogger(BenchmarkUpdateDocuments.class);


  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var directory = new File(argsItr.next());
    var keysPath = new File(argsItr.next());
    var partitions = Integer.parseInt(argsItr.next());
    var pendingWritesMergeThreshold = Integer.parseInt(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    var coreThreads = Integer.parseInt(argsItr.next());
    var updateThreads = Integer.parseInt(argsItr.next());
    var duration = Duration.parse(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());
    Preconditions.checkState(directory.isDirectory());
    Preconditions.checkState(keysPath.isFile());

    var executor = Executors.newScheduledThreadPool(coreThreads);
    try {
      var keys = loadKeys(keysPath);
      runUpdates(directory, pendingWritesMergeThreshold, partitions, indexRate, updateThreads, duration, keys, executor);
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private static void runUpdates(File directory, int pendingWritesMergeThreshold, int partitions, int indexRate, int updateThreads, Duration duration,
                                 List<DString> keys,
                                 ScheduledExecutorService executor) throws Exception {
    try (var store = BenchmarkLoadDocuments.createStore(pendingWritesMergeThreshold, indexRate, partitions,
        new FileDirectoryOperationsImpl(directory), executor)) {
      store.initialize();
      var error = new AtomicBoolean(false);
      var done = new AtomicBoolean(false);
      var updates = new AtomicLong(0);
      var threads = IntStream.range(0, updateThreads).mapToObj(i -> new Thread(() -> updateThread(error, done, updates, keys, store)))
          .collect(Collectors.toList());

      var startTime = System.nanoTime();
      var doneFuture = executor.schedule(() -> done.set(true), duration.toMillis(), TimeUnit.MILLISECONDS);
      var reportFuture = executor.scheduleWithFixedDelay(() -> report(error, updates, directory, startTime),
          0, 1, TimeUnit.SECONDS);
      threads.forEach(Thread::start);

      while (!error.get() && threads.stream().anyMatch(Thread::isAlive)) {
        for (var thread : threads) {
          thread.join(200L);
        }
      }

      if (!doneFuture.isDone()) {
        doneFuture.cancel(false);
      }

      reportFuture.cancel(false);

      for (Thread thread : threads) {
        thread.join();
      }

      report(error, updates, directory, startTime);
    }
  }

  private static void report(AtomicBoolean error, AtomicLong atomicUpdates,
                             File directory, long startTime) {
    if (error.get()) {
      return;
    }
    var time = System.nanoTime();
    var size = fileSize(directory);
    var updates = Math.max(0L, atomicUpdates.get());
    var stats = ImmutableMap.<String, Object>builder()
        .put("time", time)
        .put("updates", updates)
        .put("size", size)
        .build();
    System.out.println(new Gson().toJson(stats));
    LOGGER.info(String.format("%s updates=%.3e size=%s",
        Duration.ofNanos(time - startTime),
        (double) updates,
        Dbf0Util.formatBytes(size)));
  }

  private static void updateThread(AtomicBoolean error, AtomicBoolean done, AtomicLong updates, List<DString> keys,
                                   ReadWriteStorage<DElement, DElement> store) {
    var scoreKey = DString.of("score");
    try {
      var random = new Random();
      while (!error.get() && !done.get()) {
        var key = keys.get(random.nextInt(keys.size()));
        var value = store.get(key);
        if (value == null) {
          throw new RuntimeException("No value for " + key);
        }
        var map = (DMap) value;
        var hash = new HashMap<>(map.getEntries());
        var score = (DInt) hash.get(scoreKey);
        Preconditions.checkNotNull(score);
        hash.put(scoreKey, DInt.of(score.getValue() + 1));
        var updated = DMap.of(hash);
        store.put(key, updated);
        updates.getAndIncrement();
      }
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error in update", e);
      error.set(true);
    }
  }
}

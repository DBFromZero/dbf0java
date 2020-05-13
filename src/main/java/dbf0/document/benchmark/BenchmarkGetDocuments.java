package dbf0.document.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import dbf0.common.Dbf0Util;
import dbf0.common.EndOfStream;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.types.DElement;
import dbf0.document.types.DString;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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

import static dbf0.document.benchmark.BenchmarkLoadDocuments.fileSize;

public class BenchmarkGetDocuments {
  private static final Logger LOGGER = Dbf0Util.getLogger(BenchmarkGetDocuments.class);


  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var directory = new File(argsItr.next());
    var keysPath = new File(argsItr.next());
    var partitions = Integer.parseInt(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    var coreThreads = Integer.parseInt(argsItr.next());
    var getThreads = Integer.parseInt(argsItr.next());
    var getKeys = Long.parseLong(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());
    Preconditions.checkState(directory.isDirectory());
    Preconditions.checkState(keysPath.isFile());

    var executor = Executors.newScheduledThreadPool(coreThreads);
    try {
      var keys = new ArrayList<DString>(10000);
      try (var stream = SampleKeys.createInputStream(keysPath)) {
        var deserializer = DElementDeserializer.defaultCharsetInstance();
        while (true) {
          keys.add((DString) deserializer.deserialize(stream));
        }
      } catch (EndOfStream ignored) {
      }
      runGets(directory, partitions, indexRate, getThreads, getKeys, keys, executor);
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private static void runGets(File directory, int partitions, int indexRate, int getThreads, long getKeys,
                              List<DString> keys,
                              ScheduledExecutorService executor) throws Exception {
    try (var store = BenchmarkLoadDocuments.createStore(100000, indexRate, partitions,
        new FileDirectoryOperationsImpl(directory), executor)) {
      store.initialize();
      var error = new AtomicBoolean(false);
      var remainingGets = new AtomicLong(getKeys);
      var threads = IntStream.range(0, getThreads).mapToObj(i -> new Thread(() -> getThread(error, remainingGets, keys, store)))
          .collect(Collectors.toList());

      var startTime = System.nanoTime();
      var reportFuture = executor.scheduleWithFixedDelay(() -> report(error, remainingGets, directory, startTime),
          0, 1, TimeUnit.SECONDS);
      threads.forEach(Thread::start);

      while (!error.get() && threads.stream().anyMatch(Thread::isAlive)) {
        for (var thread : threads) {
          thread.join(200L);
        }
      }

      reportFuture.cancel(false);

      for (Thread thread : threads) {
        thread.join();
      }

      report(error, remainingGets, directory, startTime);
    }
  }

  private static void report(AtomicBoolean error, AtomicLong remainingGets,
                             File directory, long startTime) {
    if (error.get()) {
      return;
    }
    var time = System.nanoTime();
    var size = fileSize(directory);
    var remaining = Math.max(0L, remainingGets.get());
    var stats = ImmutableMap.<String, Object>builder()
        .put("time", time)
        .put("remaining", remaining)
        .put("size", size)
        .build();
    System.out.println(new Gson().toJson(stats));
    LOGGER.info(String.format("%s remaining=%.3e size=%s",
        Duration.ofNanos(time - startTime),
        (double) remaining,
        Dbf0Util.formatBytes(size)));
  }

  private static void getThread(AtomicBoolean error, AtomicLong remainingGets, List<DString> keys,
                                ReadWriteStorage<DElement, DElement> store) {
    try {
      var random = new Random();
      while (!error.get() && remainingGets.getAndDecrement() > 0) {
        var key = keys.get(random.nextInt(keys.size()));
        var value = store.get(key);
        if (value == null) {
          throw new RuntimeException("No value for " + key);
        }
      }
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error in get", e);
      error.set(true);
    }
  }
}

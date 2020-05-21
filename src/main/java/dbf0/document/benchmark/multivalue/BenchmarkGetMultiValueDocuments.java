package dbf0.document.benchmark.multivalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.tdunning.math.stats.TDigest;
import dbf0.common.Dbf0Util;
import dbf0.common.io.EndOfStream;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;
import dbf0.document.benchmark.singlevalue.SampleKeys;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.types.DElement;
import dbf0.document.types.DString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static dbf0.document.benchmark.singlevalue.BenchmarkLoadDocuments.fileSize;

public class BenchmarkGetMultiValueDocuments {
  private static final Logger LOGGER = Dbf0Util.getLogger(BenchmarkGetMultiValueDocuments.class);
  public static final List<Integer> BIN_EDGES = List.of(0, 10, 100, 1000, 10 * 1000, 100 * 1000, 1000 * 1000);


  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var directory = new File(argsItr.next());
    var keysPath = new File(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    var coreThreads = Integer.parseInt(argsItr.next());
    var getThreads = Integer.parseInt(argsItr.next());
    var duration = Duration.parse(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());
    Preconditions.checkState(directory.isDirectory());
    Preconditions.checkState(keysPath.isFile());

    int partitions = countPartitions(directory);
    Preconditions.checkState(partitions > 0, "zero partitions");
    var executor = Executors.newScheduledThreadPool(coreThreads);
    try {
      var keys = loadKeys(keysPath);
      runGets(directory, partitions, indexRate, getThreads, duration, keys, executor);
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private static int countPartitions(File directory) {
    return Dbf0Util.safeLongToInt(
        Arrays.stream(directory.list()).filter(GetKeys::isPartitionFileName).count());
  }

  @NotNull static ArrayList<DString> loadKeys(File keysPath) throws IOException {
    LOGGER.info("Loading keys");
    var keys = new ArrayList<DString>(10000);
    try (var stream = SampleKeys.createInputStream(keysPath)) {
      var deserializer = DElementDeserializer.defaultCharsetInstance();
      while (true) {
        keys.add((DString) deserializer.deserialize(stream));
      }
    } catch (EndOfStream ignored) {
    }
    LOGGER.info("Loaded " + keys.size());
    return keys;
  }

  private static void runGets(File directory, int partitions, int indexRate, int getThreads, Duration duration,
                              List<DString> keys,
                              ScheduledExecutorService executor) throws Exception {
    try (var store = BenchmarkLoadMultiValueDocuments.createStore(100000, indexRate, partitions,
        new FileDirectoryOperationsImpl(directory), executor)) {
      store.initialize();
      var error = new AtomicBoolean(false);
      var done = new AtomicBoolean(false);
      var gets = new AtomicLong(0);
      var binnedDurationQuantiles = new AtomicReference<>(new BinnedDurationQuantiles(BIN_EDGES));
      var threads = IntStream.range(0, getThreads).mapToObj(i -> new Thread(() ->
          getThread(error, done, gets, binnedDurationQuantiles, keys, store)))
          .collect(Collectors.toList());

      var startTime = System.nanoTime();
      var doneFuture = executor.schedule(() -> done.set(true), duration.toMillis(), TimeUnit.MILLISECONDS);
      var reportFuture = executor.scheduleWithFixedDelay(() ->
              report(error, gets, directory, startTime, binnedDurationQuantiles),
          0, 10, TimeUnit.SECONDS);
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

      report(error, gets, directory, startTime, binnedDurationQuantiles);
    }
  }

  private static void report(AtomicBoolean error, AtomicLong atomicGets,
                             File directory, long startTime,
                             AtomicReference<BinnedDurationQuantiles> binnedDurationQuantiles) {
    if (error.get()) {
      return;
    }
    var b = binnedDurationQuantiles.getAndSet(new BinnedDurationQuantiles(BIN_EDGES));
    var time = System.nanoTime();
    var size = fileSize(directory);
    var gets = Math.max(0L, atomicGets.get());
    var stats = ImmutableMap.<String, Object>builder()
        .put("time", time)
        .put("gets", gets)
        .put("size", size)
        .put("stats", b.jsonStats(List.of(0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0)))
        .build();
    System.out.println(new Gson().toJson(stats));
    LOGGER.info(String.format("%s gets=%.3e size=%s",
        Duration.ofNanos(time - startTime),
        (double) gets,
        Dbf0Util.formatBytes(size)));
  }

  private static void getThread(AtomicBoolean error, AtomicBoolean done, AtomicLong gets,
                                AtomicReference<BinnedDurationQuantiles> binnedDurationQuantiles, List<DString> keys,
                                MultiValueReadWriteStorage<DElement, DElement> store) {
    try {
      var random = new Random();
      while (!error.get() && !done.get()) {
        var key = keys.get(random.nextInt(keys.size()));
        int count = 0;
        long start = System.nanoTime();
        try (var value = store.get(key)) {
          if (value.maxSize() == 0) {
            throw new RuntimeException("No value for " + key);
          }
          var iterator = value.iterator();
          while (iterator.hasNext()) {
            iterator.next();
            count++;
          }
        }
        long durationNs = System.nanoTime() - start;
        gets.getAndIncrement();
        double durationMs = (double) durationNs / 1e6;
        binnedDurationQuantiles.get().record(count, durationMs);
      }
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error in get", e);
      error.set(true);
    }
  }

  private static class BinnedDurationQuantiles {

    private final TreeMap<Integer, Bin> bins = new TreeMap<>();

    public BinnedDurationQuantiles(Iterable<Integer> binEdges) {
      binEdges.forEach(i -> bins.put(i, new Bin()));
    }

    public void record(int count, double duration) {
      var bin = bins.floorEntry(count);
      if (bin == null) {
        throw new IllegalArgumentException("Bad count " + count);
      }
      bin.getValue().record(duration);
    }

    public JsonArray jsonStats(Collection<Double> quantiles) {
      var entries = new JsonArray(bins.size());
      for (var bin : bins.entrySet()) {
        var entry = bin.getValue().jsonStats(quantiles);
        if (entry != null) {
          entry.addProperty("floor", bin.getKey());
          entries.add(entry);
        }
      }
      return entries;
    }
  }

  private static class Bin {
    private final TDigest tDigest = TDigest.createDigest(100);

    private synchronized void record(double duration) {
      tDigest.add(duration);
    }

    @Nullable private synchronized JsonObject jsonStats(Collection<Double> quantiles) {
      var size = tDigest.size();
      if (size == 0) {
        return null;
      }
      var entry = new JsonObject();
      entry.addProperty("count", size);

      var values = new JsonArray(quantiles.size());
      for (var q : quantiles) {
        values.add(tDigest.quantile(q));
      }
      entry.add("quantiles", values);

      return entry;
    }
  }
}

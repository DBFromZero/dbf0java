package dbf0.document.benchmark.singlevalue;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.ReservoirSampler;
import dbf0.common.io.ByteArrayDeserializer;
import dbf0.common.io.IOUtil;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileReader;
import dbf0.disk_key_value.readonly.singlevalue.RandomAccessKeyValueFileReader;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.serialization.DElementSerializer;
import dbf0.document.types.DString;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SampleKeys {
  private static final Logger LOGGER = Dbf0Util.getLogger(SampleKeys.class);
  private static final long DIFF_BETWEEN_INDEX_LOCATION = (long) 5e7;
  private static final int MAX_BLOCKS = (int) 1e7;

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var directory = new File(argsItr.next());
    var outputPath = new File(argsItr.next());
    var partitions = Integer.parseInt(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    var coreThreads = Integer.parseInt(argsItr.next());
    var maxKeys = Integer.parseInt(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());
    Preconditions.checkState(directory.isDirectory());

    var executor = Executors.newScheduledThreadPool(coreThreads);
    try {
      var keys = loadKeys(directory, partitions, indexRate, maxKeys, executor);
      try (var stream = new BufferedOutputStream(new FileOutputStream(outputPath), 0x8000)) {
        var serializer = DElementSerializer.defaultCharsetInstance();
        for (var key : keys) {
          serializer.serialize(stream, key);
        }
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @NotNull private static List<DString> loadKeys(File directory, int partitions, int indexRate, int maxKeys,
                                                 ScheduledExecutorService executor) throws InterruptedException {
    var indexPaths = new ArrayList<File>();
    for (int partition = 0; partition < partitions; partition++) {
      var partitionDir = new File(directory, String.valueOf(partition));
      Preconditions.checkState(partitionDir.isDirectory(), "%s is not a directory", partitionDir);
      indexPaths.addAll(Arrays.asList(partitionDir.listFiles((dir, name) -> name.endsWith("-index"))));
    }
    LOGGER.info("Loading " + indexPaths.size() + " index path locations");

    var indexLocationFutures = indexPaths.stream()
        .map(indexPath -> executor.submit(() -> loadIndexLocations(indexPath, indexRate)))
        .collect(Collectors.toList());
    var indexLocations = collectSamples(indexLocationFutures, MAX_BLOCKS).getSampled();
    LOGGER.info("Sampled " + indexLocations.size() + " index locations w/ total entries of " +
        indexLocations.stream().mapToLong(e -> e.entries).sum());

    int maxKeysPerLocation = maxKeys / indexLocations.size();
    var keyFutures = indexLocations.stream()
        .map(indexLocation -> executor.submit(() -> loadKeysForIndexLocation(indexLocation, maxKeysPerLocation)))
        .collect(Collectors.toList());
    var sampledKeys = collectSamples(keyFutures, maxKeys).getSampled();
    LOGGER.info("Sampled " + sampledKeys.size() + " keys. unique " + new HashSet<>(sampledKeys).size());
    return sampledKeys;
  }

  @NotNull private static List<IndexLocation> loadIndexLocations(File indexPath, int indexRate) throws Exception {
    try {
      LOGGER.info("Loading index from " + indexPath);
      try (var indexReader = RandomAccessKeyValueFileReader.indexReader(DElementDeserializer.defaultCharsetInstance(),
          createInputStream(indexPath))) {
        var locations = new ArrayList<IndexLocation>();
        long lastEnd = 0L;
        int entries = 0;
        while (!Thread.interrupted()) {
          var key = indexReader.readKey();
          if (key == null) {
            break;
          }
          long location = indexReader.readValue();
          if (location - lastEnd >= DIFF_BETWEEN_INDEX_LOCATION) {
            locations.add(new IndexLocation(indexPath, lastEnd, indexRate * entries));
            lastEnd = location;
            entries = 0;
          }
          entries++;
        }
        if (entries > 0) {
          locations.add(new IndexLocation(indexPath, lastEnd, indexRate * entries));
        }
        return locations;
      }
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "failure in reading index", e);
      throw e;
    }
  }

  @NotNull
  private static List<DString> loadKeysForIndexLocation(IndexLocation indexLocation, int maxKeys) throws Exception {
    try {
      var path = new File(indexLocation.indexPath.toString().replace("-index", ""));
      LOGGER.info("Loading from " + path + " from " + indexLocation.start);
      try (var stream = new FileInputStream(path)) {
        IOUtil.skip(stream, indexLocation.start);
        var reader = KeyValueFileReader.bufferStream(DElementDeserializer.defaultCharsetInstance(),
            ByteArrayDeserializer.getInstance(), stream);
        var sampler = new ReservoirSampler<DString>(new Random(), maxKeys);
        int entries = 0;
        while (!Thread.interrupted() && entries++ <= indexLocation.entries) {
          var key = reader.readKey();
          if (key == null) {
            break;
          }
          sampler.add((DString) key);
          reader.skipValue();
        }
        return sampler.getSampled();
      }
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "failure in reading keys for index location", e);
      throw e;
    }
  }

  @NotNull private static <T> ReservoirSampler<T> collectSamples(
      Collection<Future<List<T>>> pendingLoadIndexLocationFutures,
      int maxSampleSize) throws InterruptedException {
    var indexLocationsSampler = new ReservoirSampler<T>(new Random(), maxSampleSize);
    boolean error = false;
    while (!pendingLoadIndexLocationFutures.isEmpty() && !error) {
      var runningLoadIndexLocationFutures = new ArrayList<Future<List<T>>>(pendingLoadIndexLocationFutures.size());
      for (var future : pendingLoadIndexLocationFutures) {
        if (error) {
          runningLoadIndexLocationFutures.add(future);
        } else {
          Collection<T> results;
          try {
            results = future.get(30, TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            runningLoadIndexLocationFutures.add(future);
            continue;
          } catch (ExecutionException e) {
            error = true;
            LOGGER.log(Level.SEVERE, "Error in future", e);
            continue;
          }
          results.forEach(indexLocationsSampler::add);
        }
      }
      pendingLoadIndexLocationFutures = runningLoadIndexLocationFutures;
    }
    if (error) {
      for (var future : pendingLoadIndexLocationFutures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      throw new RuntimeException("Error in a future");
    }
    return indexLocationsSampler;
  }

  @NotNull static BufferedInputStream createInputStream(File path) throws FileNotFoundException {
    return new BufferedInputStream(new FileInputStream(path), 0x8000);
  }

  private static class IndexLocation {
    private final File indexPath;
    private final long start;
    private final int entries;

    public IndexLocation(File indexPath, long start, int entries) {
      this.indexPath = indexPath;
      this.start = start;
      this.entries = entries;
    }
  }
}

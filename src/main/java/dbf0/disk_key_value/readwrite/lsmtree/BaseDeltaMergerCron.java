package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import dbf0.common.Dbf0Util;
import dbf0.common.io.*;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.KeyValueFileReader;
import dbf0.disk_key_value.readonly.KeyValueFileWriter;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BaseDeltaMergerCron<T extends OutputStream, K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseDeltaMergerCron.class);
  private static final int BUFFER_SIZE = 0x4000;

  private final BaseDeltaFiles<T, K, V> baseDeltaFiles;
  private final SerializationPair<K> keySerialization;
  private final SerializationPair<V> valueSerialization;
  private final Comparator<K> keyComparator;
  private final V deleteValue;
  private final double maxDeltaReadPercentage;
  private final Duration checkForDeltasRate;
  private final int baseIndexRate;
  private final ScheduledExecutorService executor;

  private boolean started = false;
  private boolean shutdown = false;
  private boolean hasError = false;
  private ScheduledFuture<?> checkFuture;

  public BaseDeltaMergerCron(BaseDeltaFiles<T, K, V> baseDeltaFiles,
                             SerializationPair<K> keySerialization,
                             SerializationPair<V> valueSerialization,
                             Comparator<K> keyComparator,
                             V deleteValue,
                             double maxDeltaReadPercentage,
                             Duration checkForDeltasRate,
                             int baseIndexRate,
                             ScheduledExecutorService executor) {
    Preconditions.checkArgument(maxDeltaReadPercentage > 0);
    Preconditions.checkArgument(maxDeltaReadPercentage < 1);
    this.baseDeltaFiles = baseDeltaFiles;
    this.keySerialization = keySerialization;
    this.valueSerialization = valueSerialization;
    this.keyComparator = keyComparator;
    this.deleteValue = deleteValue;
    this.maxDeltaReadPercentage = maxDeltaReadPercentage;
    this.checkForDeltasRate = checkForDeltasRate;
    this.baseIndexRate = baseIndexRate;
    this.executor = executor;
  }

  public boolean hasErrors() {
    return hasError;
  }

  public synchronized void start() {
    Preconditions.checkState(!started);
    checkFuture = executor.schedule(this::checkForDeltas, 0, TimeUnit.SECONDS);
    started = true;
  }

  public synchronized void shutdown() {
    if (!started || shutdown) {
      return;
    }
    if (checkFuture != null) {
      checkFuture.cancel(true);
      checkFuture = null;
    }
    shutdown = true;
  }

  public void waitForAllDeltasToMerge() throws InterruptedException {
    Preconditions.checkState(started && !hasError);
    var monitor = new Object();
    var future = executor.scheduleWithFixedDelay(() -> {
      if (baseDeltaFiles.getOrderedDeltasInUse().isEmpty()) {
        synchronized (monitor) {
          monitor.notify();
        }
      }
    }, 0, 100, TimeUnit.MILLISECONDS);
    synchronized (monitor) {
      monitor.wait();
      future.cancel(false);
    }
  }

  private void checkForDeltas() {
    if (shutdown || hasError) {
      return;
    }
    try {
      checkForDeltasInternal();
      synchronized (this) {
        if (!shutdown) {
          checkFuture = executor.schedule(this::checkForDeltas, checkForDeltasRate.toMillis(), TimeUnit.MILLISECONDS);
        }
      }
    } catch (ShutdownWhileMerging e) {
      Preconditions.checkState(shutdown);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, e, () -> "error in " + getClass().getSimpleName() + ". shutting down");
      hasError = true;
      checkFuture = null;
      shutdown();
    }
  }

  private void checkForDeltasInternal() throws IOException, ShutdownWhileMerging {
    var orderedDeltasInUse = baseDeltaFiles.getOrderedDeltasInUse();
    if (orderedDeltasInUse.isEmpty()) {
      LOGGER.finer(() -> "no deltas in use");
      return;
    }
    LOGGER.fine(() -> "ordered deltas in use " + orderedDeltasInUse);
    Preconditions.checkState(baseDeltaFiles.hasInUseBase());

    var baseOperations = baseDeltaFiles.getBaseOperations();
    var baseSize = baseOperations.length();
    var maxDeltaSize = (long) ((double) baseSize * maxDeltaReadPercentage / (1 - maxDeltaReadPercentage));
    LOGGER.fine(() -> "base size " + Dbf0Util.formatBytes(baseSize) +
        " max delta size " + Dbf0Util.formatBytes(maxDeltaSize));

    var orderedDeltaOpsForMerge = collectDeltaOpsForMerge(orderedDeltasInUse, maxDeltaSize);
    if (valueSerialization.getSerializer().canBeDeserializedAsByteArrays()) {
      // optimization to avoid de-serializing values when the value is size prefixed
      mergeDeltasAndCommit(orderedDeltaOpsForMerge, SerializationPair.forByteArrays(),
          valueSerialization.getSerializer().serializeToBytes(deleteValue));
    } else {
      mergeDeltasAndCommit(orderedDeltaOpsForMerge, valueSerialization, deleteValue);
    }
    for (var deltaPair : orderedDeltaOpsForMerge) {
      baseDeltaFiles.deleteDelta(deltaPair.getKey());
    }
  }

  @NotNull
  private List<Pair<Integer, FileOperations<T>>> collectDeltaOpsForMerge(List<Integer> orderedDeltasInUse, long maxDeltaSize) {
    List<Pair<Integer, FileOperations<T>>> deltaOpsForMerge = new ArrayList<>();
    long sumDeltaSize = 0;
    // start with the oldest deltas first and it is important to maintain order
    for (var delta : orderedDeltasInUse) {
      var deltaOps = baseDeltaFiles.getDeltaOperations(delta);
      var deltaSize = deltaOps.length();
      LOGGER.fine(() -> "Considering delta " + delta + " size " + Dbf0Util.formatBytes(deltaSize));
      var sumWithAddingDelta = sumDeltaSize + delta;
      if (sumWithAddingDelta > maxDeltaSize) {
        if (!deltaOpsForMerge.isEmpty()) {
          break;
        }
        LOGGER.warning("Merging base with just the oldest delta would exceed configured threshold. Merging anyways");
      }
      deltaOpsForMerge.add(Pair.of(delta, deltaOps));
      sumDeltaSize = sumWithAddingDelta;
    }
    return deltaOpsForMerge;
  }

  private <X> void mergeDeltasAndCommit(List<Pair<Integer, FileOperations<T>>> orderedDeltaOpsForMerge,
                                        SerializationPair<X> valueSerialization, X deleteValueX
  ) throws IOException, ShutdownWhileMerging {
    LOGGER.info(() -> "Merging base with " + orderedDeltaOpsForMerge.size() + " deltas " +
        orderedDeltaOpsForMerge.stream().map(Pair::getLeft).collect(Collectors.toList()));
    Preconditions.checkState(!orderedDeltaOpsForMerge.isEmpty());
    var baseOperations = baseDeltaFiles.getBaseOperations();
    var baseIndexOperations = baseDeltaFiles.getBaseIndexOperations();
    if (LOGGER.isLoggable(Level.FINE)) {
      var totalSize = Streams.concat(Stream.of(baseOperations), orderedDeltaOpsForMerge.stream().map(Pair::getRight))
          .mapToLong(FileOperations::length)
          .sum();
      LOGGER.fine("Total input size " + Dbf0Util.formatBytes(totalSize));
    }

    // order readers as base and the deltas in descending age such that we prefer that last
    // entry for a single key
    var orderedReaders = new ArrayList<KeyValueFileReader<K, X>>(1 + orderedDeltaOpsForMerge.size());
    FileOperations.OverWriter<T> baseOverWriter = null, baseIndexOverWriter = null;
    try {
      orderedReaders.add(batchReader(baseOperations, valueSerialization.getDeserializer()));
      for (var deltaPair : orderedDeltaOpsForMerge) {
        orderedReaders.add(batchReader(deltaPair.getRight(), valueSerialization.getDeserializer()));
      }
      var selectedIterator = ValueSelectorIterator.createSortedAndSelectedIterator(orderedReaders, keyComparator);
      baseOverWriter = baseOperations.createOverWriter();
      baseIndexOverWriter = baseIndexOperations.createOverWriter();
      try (var outputStream = new PositionTrackingStream(baseOverWriter.getOutputStream(), BUFFER_SIZE)) {
        try (var indexWriter = new KeyValueFileWriter<>(keySerialization.getSerializer(),
            UnsignedLongSerializer.getInstance(),
            new BufferedOutputStream(baseIndexOverWriter.getOutputStream(), BUFFER_SIZE))) {
          writeMerged(selectedIterator, outputStream, indexWriter, valueSerialization.getSerializer(), deleteValueX);
        }
      }
      baseDeltaFiles.commitNewBase(baseOverWriter, baseIndexOverWriter);
    } catch (Exception e) {
      if (baseOverWriter != null) {
        baseOverWriter.abort();
      }
      if (baseIndexOverWriter != null) {
        baseIndexOverWriter.abort();
      }
      if (e instanceof ShutdownWhileMerging) {
        throw e;
      }
      throw new RuntimeException("Error in merging deltas. Aborting", e);
    } finally {
      for (var reader : orderedReaders) {
        reader.close();
      }
    }
  }

  private <X> void writeMerged(ValueSelectorIterator<K, X> selectedIterator, PositionTrackingStream outputStream,
                               KeyValueFileWriter<K, Long> indexWriter, Serializer<X> valueSerialize,
                               X deleteValueX) throws IOException, ShutdownWhileMerging {
    var indexBuilder = IndexBuilder.indexBuilder(indexWriter, baseIndexRate);
    long i = 0, count = 0;
    var writer = new KeyValueFileWriter<>(keySerialization.getSerializer(), valueSerialize,
        outputStream);
    while (selectedIterator.hasNext()) {
      if (i++ % 50000 == 0) {
        if (shutdown) {
          throw new ShutdownWhileMerging();
        }
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("interrupted while merging. aborting for fast exit");
        }
        if (LOGGER.isLoggable(Level.FINER)) {
          LOGGER.finer("writing merged entry " + i + " at " + Dbf0Util.formatSize(outputStream.getPosition()));
        }
      }
      var entry = selectedIterator.next();
      if (!entry.getValue().equals(deleteValueX)) {
        indexBuilder.accept(outputStream.getPosition(), entry.getKey());
        writer.append(entry.getKey(), entry.getValue());
        count++;
      }
    }
    LOGGER.fine("wrote " + count + " key/value pairs to new base with size " +
        Dbf0Util.formatSize(outputStream.getPosition()));
  }

  private <X> KeyValueFileReader<K, X> batchReader(FileOperations<T> baseIndexFileOperations,
                                                   Deserializer<X> valueDeserializer) throws IOException {
    return new KeyValueFileReader<K, X>(keySerialization.getDeserializer(), valueDeserializer,
        new BufferedInputStream(baseIndexFileOperations.createInputStream(), BUFFER_SIZE));
  }

  private static class ShutdownWhileMerging extends Exception {
    public ShutdownWhileMerging() {
    }
  }
}

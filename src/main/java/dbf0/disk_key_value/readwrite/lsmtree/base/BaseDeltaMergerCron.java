package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import dbf0.common.Dbf0Util;
import dbf0.common.io.PositionTrackingStream;
import dbf0.disk_key_value.io.FileOperations;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BaseDeltaMergerCron<T extends OutputStream> {

  public static class ShutdownWhileMerging extends Exception {
    public ShutdownWhileMerging() {
    }
  }

  public interface ShutdownChecker {
    void checkShutdown() throws ShutdownWhileMerging;
  }

  public interface Merger {
    void merge(List<BufferedInputStream> orderedInputStreams,
               PositionTrackingStream baseOutputStream, BufferedOutputStream indexOutputStream,
               ShutdownChecker shutdownChecker)
        throws IOException, ShutdownWhileMerging;
  }

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseDeltaMergerCron.class);
  public static final int BUFFER_SIZE = 0x4000;

  private final BaseDeltaFiles<T, ?, ?, ?> baseDeltaFiles;
  private final ScheduledExecutorService executor;
  private final Duration checkFrequency;
  private final double maxDeltaReadPercentage;
  private final Merger merger;

  private boolean started = false;
  private boolean shutdown = false;
  private boolean hasError = false;
  private ScheduledFuture<?> checkFuture;

  public BaseDeltaMergerCron(BaseDeltaFiles<T, ?, ?, ?> baseDeltaFiles,
                             ScheduledExecutorService executor,
                             Duration checkFrequency,
                             double maxDeltaReadPercentage,
                             Merger merger) {
    this.baseDeltaFiles = baseDeltaFiles;
    this.executor = executor;
    this.checkFrequency = checkFrequency;
    this.maxDeltaReadPercentage = maxDeltaReadPercentage;
    this.merger = merger;
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
      checkFuture.cancel(false);
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
    }, 0, 250, TimeUnit.MILLISECONDS);
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
          checkFuture = executor.schedule(this::checkForDeltas, checkFrequency.toMillis(), TimeUnit.MILLISECONDS);
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
    mergeDeltasAndCommit(orderedDeltaOpsForMerge);
    for (var deltaPair : orderedDeltaOpsForMerge) {
      baseDeltaFiles.deleteDelta(deltaPair.getKey());
    }
  }

  @NotNull
  private List<Pair<Integer, FileOperations<T>>> collectDeltaOpsForMerge(List<Integer> orderedDeltasInUse,
                                                                         long maxDeltaSize) {
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

  private void mergeDeltasAndCommit(List<Pair<Integer, FileOperations<T>>> orderedDeltaOpsForMerge)
      throws IOException, ShutdownWhileMerging {
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

    // order stream as base and the deltas in descending age such that we prefer that last
    // entry for a single key
    var orderedInputStream = new ArrayList<BufferedInputStream>(1 + orderedDeltaOpsForMerge.size());
    FileOperations.OverWriter<T> baseOverWriter = null, baseIndexOverWriter = null;
    try {
      orderedInputStream.add(inputStream(baseOperations));
      for (var deltaPair : orderedDeltaOpsForMerge) {
        orderedInputStream.add(inputStream(deltaPair.getRight()));
      }

      baseOverWriter = baseOperations.createOverWriter();
      baseIndexOverWriter = baseIndexOperations.createOverWriter();

      try (var outputStream = new PositionTrackingStream(baseOverWriter.getOutputStream(), BUFFER_SIZE)) {
        try (var indexOutputStream = new BufferedOutputStream(baseIndexOverWriter.getOutputStream(), BUFFER_SIZE)) {
          merger.merge(orderedInputStream, outputStream, indexOutputStream, () -> {
            if (shutdown) throw new ShutdownWhileMerging();
          });
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
      for (var reader : orderedInputStream) {
        reader.close();
      }
    }
  }

  private BufferedInputStream inputStream(FileOperations<T> baseIndexFileOperations) throws IOException {
    return new BufferedInputStream(baseIndexFileOperations.createInputStream(), BUFFER_SIZE);
  }
}

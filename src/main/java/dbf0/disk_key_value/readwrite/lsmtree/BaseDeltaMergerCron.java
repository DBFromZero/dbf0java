package dbf0.disk_key_value.readwrite.lsmtree;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Streams;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.PositionTrackingStream;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.KeyValueFileIterator;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dbf0.disk_key_value.readwrite.lsmtree.LsmTree.DELETE_VALUE;

public class BaseDeltaMergerCron<T extends OutputStream> {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseDeltaMergerCron.class);
  private static final int BUFFER_SIZE = 0x4000;

  private final BaseDeltaFiles<T> baseDeltaFiles;
  private final double maxDeltaReadPercentage;
  private final Duration checkForDeltasRate;
  private final int baseIndexRate;
  private final ScheduledExecutorService executor;

  private boolean started = false;
  private boolean shutdown = false;
  private boolean hasError = false;
  private ScheduledFuture<?> checkFuture;

  public BaseDeltaMergerCron(BaseDeltaFiles<T> baseDeltaFiles,
                             double maxDeltaReadPercentage,
                             Duration checkForDeltasRate,
                             int baseIndexRate,
                             ScheduledExecutorService executor) {
    Preconditions.checkArgument(maxDeltaReadPercentage > 0);
    Preconditions.checkArgument(maxDeltaReadPercentage < 1);
    this.baseDeltaFiles = baseDeltaFiles;
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
    Preconditions.checkState(started);
    Preconditions.checkState(!shutdown);
    if (checkFuture != null) {
      checkFuture.cancel(false);
    }
    shutdown = true;
  }

  private synchronized void checkForDeltas() {
    if (shutdown || hasError) {
      return;
    }
    try {
      checkForDeltasInternal();
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, e, () -> "error checking for deltas. shutting down merging cron");
      hasError = true;
      checkFuture = null;
      if (!shutdown) {
        shutdown();
      }
    }
  }

  private void checkForDeltasInternal() throws IOException {
    var orderedDeltasInUse = baseDeltaFiles.getOrderedDeltasInUse();
    if (orderedDeltasInUse.isEmpty()) {
      LOGGER.fine(() -> "no deltas in use");
    } else {
      LOGGER.fine(() -> "ordered deltas in use " + orderedDeltasInUse);
      Preconditions.checkState(baseDeltaFiles.hasInUseBase());

      var baseOperations = baseDeltaFiles.getBaseOperations();
      var baseSize = baseOperations.length();
      var maxDeltaSize = (long) ((double) baseSize * maxDeltaReadPercentage / (1 - maxDeltaReadPercentage));
      LOGGER.fine(() -> "base size " + Dbf0Util.formatBytes(baseSize) +
          " max delta size " + Dbf0Util.formatBytes(maxDeltaSize));
      mergeDeltaAndCommit(collectDeltaOpsForMerge(orderedDeltasInUse, maxDeltaSize));
    }
    scheduleCheck();
  }

  private void scheduleCheck() {
    checkFuture = executor.schedule(this::checkForDeltas, checkForDeltasRate.toMillis(), TimeUnit.MILLISECONDS);
  }

  @NotNull
  private List<Pair<Integer, FileOperations<T>>> collectDeltaOpsForMerge(List<Integer> orderedDeltasInUse, long maxDeltaSize) {
    List<Pair<Integer, FileOperations<T>>> deltaOpsForMerge = new ArrayList<>();
    long sumDeltaSize = 0;
    // start with the oldest deltas first and it is important to maintain order
    for (var delta : Lists.reverse(orderedDeltasInUse)) {
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
      maxDeltaSize = sumWithAddingDelta;
    }
    return deltaOpsForMerge;
  }

  private void mergeDeltaAndCommit(List<Pair<Integer, FileOperations<T>>> orderedDeltaOpsForMerge) throws IOException {
    LOGGER.info("Merging base with " + orderedDeltaOpsForMerge.size() + " deltas " + orderedDeltaOpsForMerge);
    Preconditions.checkState(!orderedDeltaOpsForMerge.isEmpty());
    var baseOperations = baseDeltaFiles.getBaseOperations();
    var baseIndexOperations = baseDeltaFiles.getBaseIndexOperations();
    if (LOGGER.isLoggable(Level.FINE)) {
      var totalSize = Streams.concat(Stream.of(baseOperations), orderedDeltaOpsForMerge.stream().map(Pair::getRight))
          .mapToLong(FileOperations::length)
          .sum();
      LOGGER.fine("Total input size " + Dbf0Util.formatBytes(totalSize));
    }

    List<KeyValueFileReader> orderedReaders = new ArrayList<>();
    FileOperations.OverWriter<T> baseOverWriter = null, baseIndexOverWriter = null;
    try {
      // order readers as base and the deltas in descending age such that we prefer that last
      // entry for a single key
      orderedReaders.add(batchReader(baseOperations));
      for (var deltaPair : orderedDeltaOpsForMerge) {
        orderedReaders.add(batchReader(deltaPair.getRight()));
      }
      var selectedIterator = createSortedAndSelectedIterator(orderedReaders);
      baseOverWriter = baseOperations.createOverWriter();
      baseIndexOverWriter = baseIndexOperations.createOverWriter();
      try (var outputStream = new PositionTrackingStream(baseOverWriter.getOutputStream(), BUFFER_SIZE)) {
        try (var indexWriter = new KeyValueFileWriter(
            new BufferedOutputStream(baseIndexOverWriter.getOutputStream(), BUFFER_SIZE))) {
          writeMerged(selectedIterator, outputStream, indexWriter);
        }
      }
      baseDeltaFiles.commitNewBase(baseOverWriter, baseIndexOverWriter);
      for (var deltaPair : orderedDeltaOpsForMerge) {
        baseDeltaFiles.deleteDelta(deltaPair.getKey());
      }
    } catch (Exception e) {
      if (baseOverWriter != null) {
        baseOverWriter.abort();
      }
      if (baseIndexOverWriter != null) {
        baseIndexOverWriter.abort();
      }
      throw new RuntimeException("Error in merging deltas", e);
    } finally {
      for (var reader : orderedReaders) {
        reader.close();
      }
    }
  }

  private ValueSelectorIterator createSortedAndSelectedIterator(List<KeyValueFileReader> orderedReaders) {
    var rankedIterators = StreamUtils.zipWithIndex(orderedReaders.stream().map(KeyValueFileIterator::new))
        .map(indexedIterator -> addRank(indexedIterator.getValue(), (int) indexedIterator.getIndex()))
        .collect(Collectors.toList());
    var mergeSortedIterator = Iterators.mergeSorted(rankedIterators, Comparator.comparing(KeyValueRank::getKey));
    return new ValueSelectorIterator(mergeSortedIterator);
  }

  private void writeMerged(ValueSelectorIterator selectedIterator, PositionTrackingStream outputStream, KeyValueFileWriter indexWriter) throws IOException {
    var indexBuilder = IndexBuilder.indexBuilder(indexWriter, baseIndexRate);
    int i = 0, count = 0;
    var writer = new KeyValueFileWriter(outputStream);
    while (selectedIterator.hasNext()) {
      if (i % 10000 == 0) {
        LOGGER.fine("writing merged entry " + i);
      }
      i++;
      var entry = selectedIterator.next();
      if (!entry.getValue().equals(DELETE_VALUE)) {
        indexBuilder.accept(outputStream.getPosition(), entry.getKey());
        writer.append(entry.getKey(), entry.getValue());
        count++;
      }
    }
    LOGGER.fine("wrote " + count + " key/value pairs to new base");
  }

  private KeyValueFileReader batchReader(FileOperations<T> baseIndexFileOperations) throws IOException {
    return new KeyValueFileReader(new BufferedInputStream(baseIndexFileOperations.createInputStream(), BUFFER_SIZE));
  }

  private static Iterator<KeyValueRank> addRank(Iterator<Pair<ByteArrayWrapper, ByteArrayWrapper>> iterator, int rank) {
    return Iterators.transform(iterator, pair -> new KeyValueRank(pair.getKey(), pair.getValue(), rank));
  }

  private static class KeyValueRank {
    private final ByteArrayWrapper key;
    private final ByteArrayWrapper value;
    private final int rank;

    private KeyValueRank(ByteArrayWrapper key, ByteArrayWrapper value, int rank) {
      this.key = key;
      this.value = value;
      this.rank = rank;
    }

    private ByteArrayWrapper getKey() {
      return key;
    }
  }

  private static class ValueSelectorIterator implements Iterator<Pair<ByteArrayWrapper, ByteArrayWrapper>> {

    private final PeekingIterator<KeyValueRank> mergeSortedIterator;

    public ValueSelectorIterator(Iterator<KeyValueRank> mergeSortedIterator) {
      this.mergeSortedIterator = Iterators.peekingIterator(mergeSortedIterator);
    }

    @Override public boolean hasNext() {
      return mergeSortedIterator.hasNext();
    }

    @Override public Pair<ByteArrayWrapper, ByteArrayWrapper> next() {
      var first = mergeSortedIterator.next();
      var key = first.key;
      var highestRank = first.rank;
      var highestRankValue = first.value;
      while (mergeSortedIterator.hasNext() && mergeSortedIterator.peek().key.equals(key)) {
        var entry = mergeSortedIterator.next();
        if (entry.rank > highestRank) {
          highestRank = entry.rank;
          highestRankValue = entry.value;
        }
      }
      return Pair.of(key, highestRankValue);
    }
  }
}

package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.PositionTrackingStream;
import dbf0.common.ReadTwoStepWriteLock;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readonly.*;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LsmTree<T extends OutputStream> implements ReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper>, Closeable {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTree.class);
  private static final int BATCH_IO_BUFFER_SIZE = 0x4000;
  private static final ByteArrayWrapper DELETE_VALUE = ByteArrayWrapper.of(
      83, 76, 69, 7, 95, 21, 81, 27, 2, 104, 8, 100, 45, 109, 110, 1);

  private final int pendingWritesMergeThreshold;
  private final int baseIndexRate;
  private final FileOperations<T> baseFileOperations;
  private final FileOperations<T> baseIndexFileOperations;

  private final ReadTwoStepWriteLock lock = new ReadTwoStepWriteLock();
  private final Map<ByteArrayWrapper, ByteArrayWrapper> pendingWrites = new HashMap<>();
  private int baseSize;
  private int sizeReductionAdjustment;

  private T journalOutputStream;
  private RandomAccessKeyValueFileReader baseReader;
  private Thread baseMergeThread;
  private final AtomicBoolean mergingAborted = new AtomicBoolean(false);


  public LsmTree(int pendingWritesMergeThreshold,
                 int baseIndexRate,
                 FileOperations<T> baseFileOperations,
                 FileOperations<T> baseIndexFileOperations) {
    Preconditions.checkArgument(pendingWritesMergeThreshold > 0);
    Preconditions.checkArgument(baseIndexRate > 0);
    this.pendingWritesMergeThreshold = pendingWritesMergeThreshold;
    this.baseIndexRate = baseIndexRate;
    this.baseFileOperations = baseFileOperations;
    this.baseIndexFileOperations = baseIndexFileOperations;
  }

  public void initialize() throws IOException {
    Preconditions.checkState(journalOutputStream == null, "already initialized");
  }

  @Override public void close() throws IOException {
    Preconditions.checkState(journalOutputStream != null, "not yet initialized");
    journalOutputStream.close();
    journalOutputStream = null;
    if (baseMergeThread != null) {
      try {
        baseMergeThread.interrupt();
        baseMergeThread.join();
        baseMergeThread = null;
      } catch (InterruptedException e) {
        throw new RuntimeException("interrupted in closing vacuum thread");
      }
    }
  }

  @Override public int size() throws IOException {
    Preconditions.checkState(!mergingAborted.get());
    return lock.callWithReadLock(() -> baseSize + pendingWrites.size() - sizeReductionAdjustment);
  }

  @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(!mergingAborted.get());
    var pendingSize = lock.callWithWriteLocks(() -> {
      var oldValue = pendingWrites.put(key, value);
      if (oldValue != null && oldValue.equals(DELETE_VALUE)) {
        sizeReductionAdjustment--; // replace a delete with an actual value
      }
      if (oldValue == null) {
        var baseValue = baseReader == null ? null : baseReader.get(key);
        if (baseValue != null) {
          sizeReductionAdjustment++; // key now exists in two places, the map and the base and we only want to count it once
        }
      }
      return pendingWrites.size();
    });
    checkMergeThreshold(pendingSize);
  }

  @Nullable @Override public ByteArrayWrapper get(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(!mergingAborted.get());
    return lock.callWithReadLock(() -> {
      var value = pendingWrites.get(key);
      if (value == null && baseReader != null) {
        return baseReader.get(key);
      }
      return value;
    });
  }

  @Override public boolean delete(@NotNull ByteArrayWrapper key) throws IOException {
    // We always need to know if it's in the base
    Preconditions.checkState(!mergingAborted.get());
    return lock.callWithWriteLocks(() -> {
      var pendingValue = pendingWrites.get(key);
      if (pendingValue != null && pendingValue.equals(DELETE_VALUE)) {
        return false; // already deleted
      }
      // put already checks if in base for sizeReductionAdjustment adjustment w.r.t. base so no need to check again
      if (pendingValue == null) {
        // need to check the base to know if delete should return true and to change sizeReductionAdjustment accordingly
        // note that for large values it would be useful to have a method that just returns whether the key is in the base
        if (baseReader == null || baseReader.get(key) == null) {
          return false; // not in pendingWrites nor in base
        }
        // adjust by two to count for removing key from base for adding key w/ DELETE_VALUE to pendingWrites
        sizeReductionAdjustment++;
      }
      sizeReductionAdjustment++;
      pendingWrites.put(key, DELETE_VALUE);
      checkMergeThreshold(pendingWrites.size());
      return true;
    });
  }

  private void checkMergeThreshold(int pendingWrites) throws IOException {
    if (pendingWrites > pendingWritesMergeThreshold && baseMergeThread == null && !mergingAborted.get()) {
      LOGGER.info("Starting base merging with " + pendingWrites + " pending writes");
      var merger = new BaseMerger();
      baseMergeThread = new Thread(() -> {
        try {
          lock.runWithTwoStepWriteLocks(merger::writeNewMergedBase, merger::commit);
        } catch (Exception e) {
          mergingAborted.set(true);
          LOGGER.log(Level.SEVERE, e, () -> "error in base merging. aborting");
          merger.abort();
        } finally {
          baseMergeThread = null;
        }
      });
      baseMergeThread.start();
    }
  }

  private class BaseMerger {

    private FileOperations.OverWriter<T> baseOverWriter;
    private FileOperations.OverWriter<T> indexOverWriter;
    private int newBaseSize;

    private void writeNewMergedBase() throws IOException {
      Preconditions.checkState(baseOverWriter == null);
      baseOverWriter = baseFileOperations.createOverWriter();
      indexOverWriter = baseIndexFileOperations.createOverWriter();
      try (var baseReader = baseFileOperations.exists() ? batchReader(baseFileOperations) :
          new KeyValueFileReader(new ByteArrayInputStream(new byte[0]))) {
        try (var outputStream = new PositionTrackingStream(baseOverWriter.getOutputStream(), BATCH_IO_BUFFER_SIZE)) {
          try (var indexWriter = new KeyValueFileWriter(
              new BufferedOutputStream(indexOverWriter.getOutputStream(), BATCH_IO_BUFFER_SIZE))) {
            var indexBuilder = IndexBuilder.indexBuilder(indexWriter, baseIndexRate);
            newBaseSize = mergePendingWritesWithBase(baseReader, outputStream, indexBuilder);
          }
        }
      }
    }

    private void commit() throws IOException {
      LOGGER.info("Committing new merged base with " + newBaseSize + " entries");
      Preconditions.checkState(baseOverWriter != null);
      baseOverWriter.commit();
      baseOverWriter = null;
      indexOverWriter.commit();
      indexOverWriter = null;
      pendingWrites.clear();
      sizeReductionAdjustment = 0;
      baseSize = newBaseSize;
      baseReader = new RandomAccessKeyValueFileReader(baseFileOperations, RandomAccessKeyValueFileReader.readIndex(
          new KeyValueFileIterator(batchReader(baseIndexFileOperations))));
    }

    private void abort() {
      Preconditions.checkState(baseOverWriter != null);
      baseOverWriter.abort();
      baseOverWriter = null;
      indexOverWriter.abort();
      indexOverWriter = null;
    }
  }

  @NotNull private KeyValueFileReader batchReader(FileOperations<T> baseIndexFileOperations) throws IOException {
    return new KeyValueFileReader(new BufferedInputStream(baseIndexFileOperations.createInputStream(), BATCH_IO_BUFFER_SIZE));
  }

  private int mergePendingWritesWithBase(KeyValueFileReader baseReader, PositionTrackingStream outputStream,
                                         IndexBuilder indexBuilder) throws IOException {
    LOGGER.info("sorting pending write of size " + pendingWrites.size());
    var sortedPendingIterator = pendingWrites.entrySet().stream().sorted(Map.Entry.comparingByKey())
        .map(entry -> new SourceKV(KeyValueSource.PENDING, entry.getKey(), entry.getValue()))
        .iterator();
    var sortedBaseIterator = Iterators.transform(new KeyValueFileIterator(baseReader),
        (kvPair) -> new SourceKV(KeyValueSource.BASE, kvPair.getKey(), kvPair.getValue()));
    var mergeSortedIterator = Iterators.mergeSorted(List.of(sortedBaseIterator, sortedPendingIterator),
        Comparator.comparing(SourceKV::getKey));
    var selectedIterator = new PendingSelectorIterator(Iterators.peekingIterator(mergeSortedIterator));
    int i = 0, count = 0;
    LOGGER.info("writing new base");
    var writer = new KeyValueFileWriter(outputStream);
    while (selectedIterator.hasNext()) {
      if (i % 1000 == 0) {
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
    LOGGER.info("wrote " + count + " key/value pairs to new base");
    return count;
  }

  private enum KeyValueSource {
    PENDING,
    BASE
  }

  private static final class SourceKV {
    private final KeyValueSource source;
    private final ByteArrayWrapper key;
    private final ByteArrayWrapper value;

    public SourceKV(KeyValueSource source, ByteArrayWrapper key, ByteArrayWrapper value) {
      this.source = source;
      this.key = key;
      this.value = value;
    }

    public ByteArrayWrapper getKey() {
      return key;
    }
  }

  private static final class PendingSelectorIterator implements Iterator<Pair<ByteArrayWrapper, ByteArrayWrapper>> {

    private final PeekingIterator<SourceKV> mergeSortedIterator;
    private Pair<ByteArrayWrapper, ByteArrayWrapper> next;

    public PendingSelectorIterator(PeekingIterator<SourceKV> mergeSortedIterator) {
      this.mergeSortedIterator = mergeSortedIterator;
    }

    @Override public boolean hasNext() {
      return mergeSortedIterator.hasNext();
    }

    @Override public Pair<ByteArrayWrapper, ByteArrayWrapper> next() {
      var first = mergeSortedIterator.next();
      if (!mergeSortedIterator.hasNext()) {
        return Pair.of(first.key, first.value);
      }
      var second = mergeSortedIterator.peek();
      if (!second.key.equals(first.key)) {
        return Pair.of(first.key, first.value);
      }
      mergeSortedIterator.next();
      if (mergeSortedIterator.hasNext()) {
        Preconditions.checkState(!second.key.equals(mergeSortedIterator.peek().key), "more than two entries for %s", second.key);
      }
      if (first.source == KeyValueSource.PENDING) {
        Preconditions.checkState(second.source == KeyValueSource.BASE);
        return Pair.of(first.key, first.value);
      } else {
        Preconditions.checkState(first.source == KeyValueSource.BASE);
        Preconditions.checkState(second.source == KeyValueSource.PENDING);
        return Pair.of(first.key, second.value);
      }
    }
  }
}

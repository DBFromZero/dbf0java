package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.readwrite.CloseableReadWriteStorage;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class LsmTree<T extends OutputStream> implements CloseableReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTree.class);
  static final ByteArrayWrapper DELETE_VALUE = ByteArrayWrapper.of(
      83, 76, 69, 7, 95, 21, 81, 27, 2, 104, 8, 100, 45, 109, 110, 1);

  private final int pendingWritesMergeThreshold;
  private final BaseDeltaFiles<T> baseDeltaFiles;
  private final DeltaWriterCoordinator<T> coordinator;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private Map<ByteArrayWrapper, ByteArrayWrapper> pendingWrites = new HashMap<>();

  public LsmTree(int pendingWritesMergeThreshold,
                 BaseDeltaFiles<T> baseDeltaFiles,
                 DeltaWriterCoordinator<T> coordinator) {
    Preconditions.checkArgument(pendingWritesMergeThreshold > 0);
    this.pendingWritesMergeThreshold = pendingWritesMergeThreshold;
    this.baseDeltaFiles = baseDeltaFiles;
    this.coordinator = coordinator;
  }

  public void initialize() throws IOException {
    coordinator.initialize();
  }

  @Override public void close() throws IOException {
    coordinator.shutdown();
  }

  @Override public int size() throws IOException {
    throw new RuntimeException("not implemented");
  }

  @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(coordinator.noWritesAborted());
    var writesBlocked = lock.callWithWriteLock(() -> {
      pendingWrites.put(key, value);
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
  }

  @Nullable @Override public ByteArrayWrapper get(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(coordinator.noWritesAborted());
    var outerValue = lock.callWithReadLock(() -> {
      var value = pendingWrites.get(key);
      if (value != null) {
        return value;
      }
      return coordinator.searchForKeyInWritesInProgress(key);
    });
    if (outerValue != null) {
      return outerValue.equals(DELETE_VALUE) ? null : outerValue;
    }
    return baseDeltaFiles.hasInUseBase() ? baseDeltaFiles.searchForKey(key) : null;
  }

  @Override public boolean delete(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(coordinator.noWritesAborted());
    var writesBlocked = lock.callWithWriteLock(() -> {
      pendingWrites.put(key, DELETE_VALUE);
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
    return true;// doesn't actually return a useful value
  }

  // only to be called when holding the write lock
  private boolean checkMergeThreshold() throws IOException {
    if (pendingWrites.size() < pendingWritesMergeThreshold) {
      return false;
    }
    if (coordinator.hasMaxInFlightWriters()) {
      return true;
    }
    coordinator.addWrites(Collections.unmodifiableMap(pendingWrites));
    pendingWrites = new HashMap<>();
    return false;
  }

  private void waitForWritesToUnblock() {
    LOGGER.warning("There are too many in-flight write jobs. Waiting for them to finish");
    try {
      while (coordinator.hasMaxInFlightWriters()) {
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedExceptionWrapper("interrupted waiting for write jobs to finish");
    }
  }

  /*
  private class BaseMerger {

    private final Map<ByteArrayWrapper, ByteArrayWrapper> writes;

    public BaseMerger(Map<ByteArrayWrapper, ByteArrayWrapper> writes) {
      this.writes = writes;
    }

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

    private int mergePendingWritesWithBase(KeyValueFileReader baseReader, PositionTrackingStream outputStream,
                                           IndexBuilder indexBuilder) throws IOException {
      LOGGER.info("sorting pending write of size " + writes.size());
      var sortedPendingIterator = writes.entrySet().stream().sorted(Map.Entry.comparingByKey())
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

    private void commit() throws IOException {
      LOGGER.info("Committing new merged base with " + newBaseSize + " entries");
      Preconditions.checkState(baseOverWriter != null);
      baseOverWriter.commit();
      baseOverWriter = null;
      indexOverWriter.commit();
      indexOverWriter = null;
      writesInProgress = Collections.emptyMap();
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

   */
}

package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadWriteLockHelper;
import dbf0.common.io.Serializer;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeltaWriterCoordinator<T extends OutputStream, K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(DeltaWriterCoordinator.class);

  private enum State {
    UNINITIALIZED,
    WRITING_BASE,
    WRITE_DELTAS
  }

  private final BaseDeltaFiles<T, K, V> baseDeltaFiles;
  private final int maxInFlightWriters;
  private final int indexRate;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final Comparator<K> keyComparator;
  private final ScheduledExecutorService executor;
  @Nullable private final WriteAheadLog<?> writeAheadLog;

  private final LinkedList<WriteSortedEntriesJob<T, K, V>> inFlightWriters = new LinkedList<>();
  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private int writersCreated = 0;
  private boolean anyWriteAborted = false;
  private State state = State.UNINITIALIZED;

  public DeltaWriterCoordinator(BaseDeltaFiles<T, K, V> baseDeltaFiles, int indexRate, int maxInFlightWriters,
                                Serializer<K> keySerializer,
                                Serializer<V> valueSerializer,
                                Comparator<K> keyComparator,
                                ScheduledExecutorService executor,
                                @Nullable WriteAheadLog<?> writeAheadLog) {
    Preconditions.checkArgument(indexRate > 0);
    Preconditions.checkArgument(maxInFlightWriters > 1);
    Preconditions.checkArgument(maxInFlightWriters < 100);
    this.baseDeltaFiles = Preconditions.checkNotNull(baseDeltaFiles);
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyComparator = keyComparator;
    this.maxInFlightWriters = maxInFlightWriters;
    this.indexRate = indexRate;
    this.executor = executor;
    this.writeAheadLog = writeAheadLog;
  }

  boolean isUsable() {
    return !anyWriteAborted;
  }

  boolean hasMaxInFlightWriters() {
    return lock.callWithReadLockUnchecked(inFlightWriters::size) == maxInFlightWriters;
  }

  boolean hasInFlightWriters() {
    return !lock.callWithReadLockUnchecked(inFlightWriters::isEmpty);
  }

  void addWrites(PendingWritesAndLog<K, V> originalWrites) {
    Preconditions.checkState(!hasMaxInFlightWriters());
    Preconditions.checkState(isUsable());
    var writes = new PendingWritesAndLog<>(Collections.unmodifiableMap(originalWrites.writes), originalWrites.logWriter);
    lock.runWithWriteLockUnchecked(() -> {
      if (state == State.UNINITIALIZED) {
        if (baseDeltaFiles.hasInUseBase()) {
          state = State.WRITE_DELTAS;
        } else {
          LOGGER.info("Creating new base for writes");
          Preconditions.checkState(!baseDeltaFiles.baseFileExists());
          state = State.WRITING_BASE;
          createWriteJob(true, -1, writes, baseDeltaFiles.getBaseOperations(),
              baseDeltaFiles.getBaseIndexOperations());
          return;
        }
      }
      LOGGER.info("Creating new delta for writes");
      var delta = baseDeltaFiles.allocateDelta();
      createWriteJob(false, delta, writes, baseDeltaFiles.getDeltaOperations(delta),
          baseDeltaFiles.getDeltaIndexOperations(delta));
    });
  }

  @Nullable V searchForKeyInWritesInProgress(K key) {
    Preconditions.checkState(executor != null, "not initialized");
    return lock.callWithReadLockUnchecked(() -> {
      // search the newest writes first
      var iterator = inFlightWriters.descendingIterator();
      while (iterator.hasNext()) {
        var writer = iterator.next();
        var value = writer.getPendingWritesAndLog().writes.get(key);
        if (value != null) {
          return value;
        }
      }
      return null;
    });
  }

  private void createWriteJob(boolean isBase, int delta,
                              PendingWritesAndLog<K, V> writesAndLog,
                              FileOperations<T> fileOperations,
                              FileOperations<T> indexFileOperations) {
    var job = new WriteSortedEntriesJob<>("write" + writersCreated++,
        isBase, delta, indexRate,
        keySerializer, valueSerializer, keyComparator,
        writesAndLog, fileOperations, indexFileOperations, this);
    inFlightWriters.add(job);
    executor.execute(job);
  }

  void commitWrites(WriteSortedEntriesJob<T, K, V> writer) throws IOException {
    if (anyWriteAborted) {
      LOGGER.warning("Not committing " + writer.getName() + " since an earlier writer aborted");
      abortWrites(writer);
    } else {
      lock.runWithWriteLockUnchecked(() -> {
        if (state == State.WRITING_BASE && !writer.isBase()) {
          // we cannot write a delta before writing the base so wait
          LOGGER.info(() -> "Writer " + writer.getName() + " finished before the initial base finished. " +
              "Will re-attempt to commit this later");
          executor.schedule(() -> reattemptCommitWrites(writer, 1), 1, TimeUnit.SECONDS);
        } else {
          commitWritesWithLogging(writer);
        }
      });
    }
    synchronized (this) {
      notifyAll(); // LSMTree awaits writes to finish when shutting down or when at capacity
    }
  }

  private void commitWritesWithLogging(WriteSortedEntriesJob<T, K, V> writer) {
    try {
      if (state == State.WRITING_BASE) {
        Preconditions.checkState(writer.isBase());
        baseDeltaFiles.setBase();
        state = State.WRITE_DELTAS;
      } else {
        Preconditions.checkState(state == State.WRITE_DELTAS);
        baseDeltaFiles.addDelta(writer.getDelta());
      }
      var logWriter = writer.getPendingWritesAndLog().logWriter;
      if (logWriter != null) {
        Preconditions.checkState(writeAheadLog != null);
        writeAheadLog.freeWriter(logWriter.getName());
      }
      var removed = inFlightWriters.remove(writer);
      Preconditions.checkState(removed);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, e, () -> "error in committing writes. aborting");
      abortWrites(writer);
    }
  }

  void reattemptCommitWrites(WriteSortedEntriesJob<T, K, V> writer, int count) {
    LOGGER.info(() -> "Reattempting " + writer.getName() + " count=" + count);
    Preconditions.checkState(!writer.isBase());
    if (state == State.WRITING_BASE) {
      if (count > 5) {
        LOGGER.warning("Failed to commit " + writer.getName() + " after " + count + "attempts. Aborting");
        abortWrites(writer);
      } else {
        LOGGER.info("Still writing base after " + count + " attempts. Will reattempt commit of " + writer.getName());
        executor.schedule(() -> reattemptCommitWrites(writer, count + 1), 2 << count, TimeUnit.SECONDS);
      }
      return;
    }
    try {
      Preconditions.checkState(state == State.WRITE_DELTAS);
      commitWrites(writer);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, e, () -> "error in committing writes. aborting");
      abortWrites(writer);
    }
  }

  void abortWrites(WriteSortedEntriesJob<T, K, V> writer) {
    anyWriteAborted = true;
    LOGGER.warning("Aborting " + writer.getName());
    lock.runWithWriteLockUnchecked(() -> {
      var removed = inFlightWriters.remove(writer);
      Preconditions.checkState(removed);
    });
  }
}

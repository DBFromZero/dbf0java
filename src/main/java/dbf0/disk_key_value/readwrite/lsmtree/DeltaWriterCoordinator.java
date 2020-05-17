package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.FixedSizeBackgroundJobCoordinator;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import org.jetbrains.annotations.Nullable;

import java.io.OutputStream;
import java.util.Collections;
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

  private final LsmTreeConfiguration<K, V> configuration;
  private final BaseDeltaFiles<T, K, V> baseDeltaFiles;
  private final ScheduledExecutorService executor;
  private final FixedSizeBackgroundJobCoordinator<WriteSortedEntriesJob<T, K, V>> backgroundJobCoordinator;
  @Nullable private final WriteAheadLog<?> writeAheadLog;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private int writersCreated = 0;
  private boolean anyWriteAborted = false;
  private State state = State.UNINITIALIZED;

  public DeltaWriterCoordinator(LsmTreeConfiguration<K, V> configuration,
                                BaseDeltaFiles<T, K, V> baseDeltaFiles,
                                ScheduledExecutorService executor,
                                @Nullable WriteAheadLog<?> writeAheadLog) {
    this.configuration = configuration;
    this.baseDeltaFiles = baseDeltaFiles;
    this.executor = executor;
    this.writeAheadLog = writeAheadLog;
    this.backgroundJobCoordinator = new FixedSizeBackgroundJobCoordinator<>(executor, configuration.getMaxInFlightWriteJobs());
  }

  boolean isUsable() {
    return !anyWriteAborted;
  }

  boolean hasMaxInFlightWriters() {
    return backgroundJobCoordinator.hasMaxInFlightJobs();
  }

  boolean hasInFlightWriters() {
    return backgroundJobCoordinator.hasInFlightJobs();
  }

  void addWrites(PendingWritesAndLog<K, V> originalWrites) {
    Preconditions.checkState(!hasMaxInFlightWriters());
    Preconditions.checkState(isUsable());
    var writes = new PendingWritesAndLog<>(Collections.unmodifiableMap(originalWrites.writes), originalWrites.logWriter);
    synchronized (this) {
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
    }
  }

  public void awaitNextJobCompletion() throws InterruptedException {
    backgroundJobCoordinator.awaitNextJobCompletion();
  }

  @Nullable V searchForKeyInWritesInProgress(K key) {
    var jobs = backgroundJobCoordinator.getCurrentInFlightJobs();
    for (int i = jobs.size() - 1; i >= 0; i--) {
      var value = jobs.get(i).getPendingWritesAndLog().writes.get(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private void createWriteJob(boolean isBase, int delta,
                              PendingWritesAndLog<K, V> writesAndLog,
                              FileOperations<T> fileOperations,
                              FileOperations<T> indexFileOperations) {
    var job = new WriteSortedEntriesJob<>("write" + writersCreated++,
        isBase, delta, configuration,
        writesAndLog, fileOperations, indexFileOperations, this);
    backgroundJobCoordinator.execute(job);
  }

  void commitWrites(WriteSortedEntriesJob<T, K, V> writer) {
    if (anyWriteAborted) {
      LOGGER.warning("Not committing " + writer.getName() + " since an earlier writer aborted");
      abortWrites(writer);
    } else {
      synchronized (this) {
        if (state == State.WRITING_BASE && !writer.isBase()) {
          // we cannot write a delta before writing the base so wait
          LOGGER.info(() -> "Writer " + writer.getName() + " finished before the initial base finished. " +
              "Will re-attempt to commit this later");
          executor.schedule(() -> reattemptCommitWrites(writer, 1), 1, TimeUnit.SECONDS);
        } else {
          commitWritesWithLogging(writer);
        }
      }
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
  }
}

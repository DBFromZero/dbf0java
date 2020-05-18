package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.FixedSizeBackgroundJobCoordinator;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import org.jetbrains.annotations.Nullable;

import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WriteJobCoordinator<T extends OutputStream, W, P extends PendingWrites<W>> {

  private static final Logger LOGGER = Dbf0Util.getLogger(WriteJobCoordinator.class);

  private enum State {
    UNINITIALIZED,
    WRITING_BASE,
    WRITE_DELTAS
  }

  private final BaseDeltaFiles<T, ?, ?, ?> baseDeltaFiles;
  private final ScheduledExecutorService executor;
  @Nullable private final WriteAheadLog<?> writeAheadLog;
  private final WriteJob.SortAndWriter<T, W> writer;
  private final FixedSizeBackgroundJobCoordinator<WriteJob<T, W, P>> backgroundJobCoordinator;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private int writersCreated = 0;
  private boolean anyWriteAborted = false;
  private State state = State.UNINITIALIZED;

  public WriteJobCoordinator(BaseDeltaFiles<T, ?, ?, ?> baseDeltaFiles,
                             ScheduledExecutorService executor,
                             @Nullable WriteAheadLog<?> writeAheadLog,
                             WriteJob.SortAndWriter<T, W> writer,
                             int maxInFlightJobs) {
    this.baseDeltaFiles = baseDeltaFiles;
    this.executor = executor;
    this.writeAheadLog = writeAheadLog;
    this.writer = writer;
    this.backgroundJobCoordinator = new FixedSizeBackgroundJobCoordinator<>(executor, maxInFlightJobs);
  }

  public boolean isUsable() {
    return !anyWriteAborted;
  }

  public boolean hasMaxInFlightWriters() {
    return backgroundJobCoordinator.hasMaxInFlightJobs();
  }

  public boolean hasInFlightWriters() {
    return backgroundJobCoordinator.hasInFlightJobs();
  }

  public void addWrites(P writes) {
    Preconditions.checkState(!hasMaxInFlightWriters());
    Preconditions.checkState(isUsable());
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

  public List<WriteJob<T, W, P>> getCurrentInFlightJobs() {
    return backgroundJobCoordinator.getCurrentInFlightJobs();
  }

  private void createWriteJob(boolean isBase, int delta, P writes,
                              FileOperations<T> fileOperations,
                              FileOperations<T> indexFileOperations) {
    var job = new WriteJob<>("write" + writersCreated++, isBase, delta, writes,
        fileOperations, indexFileOperations, this, writer);
    backgroundJobCoordinator.execute(job);
  }

  void commitWrites(WriteJob<T, W, P> writer) {
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

  private void commitWritesWithLogging(WriteJob<T, W, P> writer) {
    try {
      if (state == State.WRITING_BASE) {
        Preconditions.checkState(writer.isBase());
        baseDeltaFiles.setBase();
        state = State.WRITE_DELTAS;
      } else {
        Preconditions.checkState(state == State.WRITE_DELTAS);
        baseDeltaFiles.addDelta(writer.getDelta());
      }
      writer.getPendingWrites().freeWriteAheadLog();
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, e, () -> "error in committing writes. aborting");
      abortWrites(writer);
    }
  }

  private void reattemptCommitWrites(WriteJob<T, W, P> writer, int count) {
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

  void abortWrites(WriteJob<T, W, P> writer) {
    anyWriteAborted = true;
    LOGGER.warning("Aborting " + writer.getName());
  }
}

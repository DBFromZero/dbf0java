package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.readwrite.CloseableReadWriteStorage;
import dbf0.disk_key_value.readwrite.ReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.log.LogConsumer;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class LsmTree<T extends OutputStream> implements CloseableReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTree.class);
  static final ByteArrayWrapper DELETE_VALUE = ByteArrayWrapper.of(
      83, 76, 69, 7, 95, 21, 81, 27, 2, 104, 8, 100, 45, 109, 110, 1);

  public static class Builder<T extends OutputStream> {
    private int pendingWritesDeltaThreshold = 10 * 1000;
    private BaseDeltaFiles<T> baseDeltaFiles;
    private DeltaWriterCoordinator<T> deltaWriterCoordinator;
    private BaseDeltaMergerCron<T> mergerCron;
    private ScheduledExecutorService executorService;
    private WriteAheadLog<?> writeAheadLog;


    private int indexRate = 10;
    private int maxInFlightWriteJobs = 10;
    double maxDeltaReadPercentage = 0.5;
    private Duration mergeCronFrequency = Duration.ofSeconds(1);

    public Builder() {
    }

    public Builder<T> withPendingWritesDeltaThreshold(int pendingWritesDeltaThreshold) {
      Preconditions.checkArgument(pendingWritesDeltaThreshold > 0);
      this.pendingWritesDeltaThreshold = pendingWritesDeltaThreshold;
      return this;
    }

    public Builder<T> withBaseDeltaFiles(BaseDeltaFiles<T> baseDeltaFiles) {
      this.baseDeltaFiles = Preconditions.checkNotNull(baseDeltaFiles);
      return this;
    }

    public Builder<T> withBaseDeltaFiles(FileDirectoryOperations<T> fileDirectoryOperations) {
      return withBaseDeltaFiles(new BaseDeltaFiles<>(fileDirectoryOperations));
    }

    public Builder<T> withDeltaWriteCoordinator(DeltaWriterCoordinator<T> coordinator) {
      this.deltaWriterCoordinator = Preconditions.checkNotNull(coordinator);
      return this;
    }

    public Builder<T> withMergerCron(BaseDeltaMergerCron<T> mergerCron) {
      this.mergerCron = Preconditions.checkNotNull(mergerCron);
      return this;
    }

    public Builder<T> withScheduledExecutorService(ScheduledExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public Builder<T> withScheduledThreadPool(int corePoolSize) {
      return withScheduledExecutorService(Executors.newScheduledThreadPool(corePoolSize));
    }

    public Builder<T> withWriteAheadLog(WriteAheadLog<?> writeAheadLog) {
      this.writeAheadLog = writeAheadLog;
      return this;
    }

    public Builder<T> withIndexRate(int indexRate) {
      Preconditions.checkArgument(indexRate > 0);
      this.indexRate = indexRate;
      return this;
    }

    public Builder<T> withMaxInFlightWriteJobs(int maxInFlightWriteJobs) {
      Preconditions.checkArgument(maxInFlightWriteJobs > 0);
      this.maxInFlightWriteJobs = maxInFlightWriteJobs;
      return this;
    }

    public Builder<T> withMaxDeltaReadPercentage(double maxDeltaReadPercentage) {
      Preconditions.checkArgument(maxDeltaReadPercentage > 0);
      Preconditions.checkArgument(maxDeltaReadPercentage < 1);
      this.maxDeltaReadPercentage = maxDeltaReadPercentage;
      return this;
    }

    public Builder<T> withMergeCronFrequency(Duration mergeCronFrequency) {
      Preconditions.checkArgument(!mergeCronFrequency.isZero());
      Preconditions.checkArgument(!mergeCronFrequency.isNegative());
      this.mergeCronFrequency = mergeCronFrequency;
      return this;
    }

    public LsmTree<T> build() {
      return buildInternal().getLeft();
    }

    public ReadWriteStorageWithBackgroundTasks<ByteArrayWrapper, ByteArrayWrapper> buildWithBackgroundTaks() {
      var pair = buildInternal();
      return new ReadWriteStorageWithBackgroundTasks<>(pair.getLeft(), pair.getRight());
    }

    private Pair<LsmTree<T>, ExecutorService> buildInternal() {
      Preconditions.checkState(baseDeltaFiles != null, "must specify baseDeltaFiles");
      ScheduledExecutorService executorService = this.executorService;
      if (executorService == null) {
        executorService = Executors.newScheduledThreadPool(4);
      }
      DeltaWriterCoordinator<T> coordinator = this.deltaWriterCoordinator;
      if (coordinator == null) {
        coordinator = new DeltaWriterCoordinator<>(baseDeltaFiles, indexRate, maxInFlightWriteJobs,
            executorService, writeAheadLog);
      }
      BaseDeltaMergerCron<T> mergerCron = this.mergerCron;
      if (mergerCron == null) {
        mergerCron = new BaseDeltaMergerCron<>(baseDeltaFiles, maxDeltaReadPercentage, mergeCronFrequency,
            indexRate, executorService);
      }
      return Pair.of(new LsmTree<>(pendingWritesDeltaThreshold, baseDeltaFiles, coordinator, mergerCron, writeAheadLog),
          executorService);
    }
  }

  public static <T extends OutputStream> Builder<T> builder() {
    return new Builder<T>();
  }

  public static Builder<FileOutputStream> builderForDirectory(FileDirectoryOperations<FileOutputStream> directoryOps) {
    Builder<FileOutputStream> builder = builder();
    return builder.withBaseDeltaFiles(directoryOps);
  }

  public static Builder<FileOutputStream> builderForDirectory(File directory) {
    return builderForDirectory(new FileDirectoryOperationsImpl(directory));
  }

  public static Builder<MemoryFileOperations.MemoryOutputStream> builderForTesting(
      MemoryFileDirectoryOperations directoryOperations) {
    Builder<MemoryFileOperations.MemoryOutputStream> builder = builder();
    return builder.withBaseDeltaFiles(directoryOperations);
  }

  public static Builder<MemoryFileOperations.MemoryOutputStream> builderForTesting() {
    return builderForTesting(new MemoryFileDirectoryOperations());
  }

  private final int pendingWritesDeltaThreshold;
  private final BaseDeltaFiles<T> baseDeltaFiles;
  private final DeltaWriterCoordinator<T> coordinator;
  private final BaseDeltaMergerCron<T> mergerCron;
  @Nullable private final WriteAheadLog<?> writeAheadLog;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private PendingWritesAndLog pendingWrites;

  private LsmTree(int pendingWritesDeltaThreshold,
                  BaseDeltaFiles<T> baseDeltaFiles,
                  DeltaWriterCoordinator<T> coordinator,
                  BaseDeltaMergerCron<T> mergerCron,
                  @Nullable WriteAheadLog<?> writeAheadLog) {
    this.pendingWritesDeltaThreshold = pendingWritesDeltaThreshold;
    this.baseDeltaFiles = baseDeltaFiles;
    this.coordinator = coordinator;
    this.mergerCron = mergerCron;
    this.writeAheadLog = writeAheadLog;
  }

  @Override public void initialize() throws IOException {
    baseDeltaFiles.loadExisting();

    if (writeAheadLog != null) {
      writeAheadLog.initialize(() -> {
        // these pending writes don't need a log since they are already persisted in current log that we're reading
        pendingWrites = new PendingWritesAndLog(new HashMap<>(pendingWritesDeltaThreshold), null);
        return new LogConsumer() {
          @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
            pendingWrites.writes.put(key, value);
          }

          @Override public void delete(@NotNull ByteArrayWrapper key) throws IOException {
            pendingWrites.writes.put(key, DELETE_VALUE);
          }

          @Override public void persist() throws IOException {
            sendWritesToCoordinator();
            coordinator.addWrites(pendingWrites);
            pendingWrites = new PendingWritesAndLog(new HashMap<>(pendingWritesDeltaThreshold), null);
          }
        };
      });
    }

    mergerCron.start();
    createNewPendingWrites();
  }


  @Override public void close() throws IOException {
    mergerCron.shutdown();
    if (isUsable()) {
      lock.runWithWriteLock(() -> {
        if (!pendingWrites.writes.isEmpty()) {
          if (coordinator.hasMaxInFlightWriters()) {
            LOGGER.warning("Delaying close because DeltaWriteCoordinator is backed up");
            waitForWritesToUnblock();
          }
          LOGGER.info("writing pending at close");
          sendWritesToCoordinator();
        }
        pendingWrites = null;

        try {
          while (coordinator.hasInFlightWriters() && coordinator.isUsable()) {
            LOGGER.info("Waiting for deltas to finish writing");
            synchronized (coordinator) {
              coordinator.wait();
            }
          }
        } catch (InterruptedException e) {
          LOGGER.warning("Interrupted waiting for deltas to finish writing");
        }
      });
    }
  }

  @Override public long size() throws IOException {
    throw new RuntimeException("not implemented");
  }

  public boolean isUsable() {
    return coordinator.isUsable() && !mergerCron.hasErrors();
  }

  @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (pendingWrites.logWriter != null) {
        pendingWrites.logWriter.logPut(key, value);
      }
      pendingWrites.writes.put(key, value);
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
  }

  @Nullable @Override public ByteArrayWrapper get(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(isUsable());
    // search through the various containers that could contain the key in the appropriate order
    var value = lock.callWithReadLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      return pendingWrites.writes.get(key);
    });
    if (value != null) {
      return checkForDeleteValue(value);
    }
    value = coordinator.searchForKeyInWritesInProgress(key);
    if (value != null) {
      return checkForDeleteValue(value);
    }
    if (!baseDeltaFiles.hasInUseBase()) {
      return null;
    }
    value = baseDeltaFiles.searchForKey(key);
    return value == null ? null : checkForDeleteValue(value);
  }

  private @Nullable ByteArrayWrapper checkForDeleteValue(@NotNull ByteArrayWrapper value) {
    return value.equals(DELETE_VALUE) ? null : value;
  }

  @Override public boolean delete(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (pendingWrites.logWriter != null) {
        pendingWrites.logWriter.logDelete(key);
      }
      pendingWrites.writes.put(key, DELETE_VALUE);
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
    return true;// doesn't actually return a useful value
  }

  // only to be called when holding the write lock
  private boolean checkMergeThreshold() throws IOException {
    if (pendingWrites.writes.size() < pendingWritesDeltaThreshold) {
      return false;
    }
    if (coordinator.hasMaxInFlightWriters()) {
      return true;
    }
    sendWritesToCoordinator();
    createNewPendingWrites();
    return false;
  }

  private void createNewPendingWrites() throws IOException {
    pendingWrites = new PendingWritesAndLog(new HashMap<>(pendingWritesDeltaThreshold),
        writeAheadLog == null ? null : writeAheadLog.createWriter());
  }

  private void sendWritesToCoordinator() throws IOException {
    if (pendingWrites.logWriter != null) {
      pendingWrites.logWriter.close();
    }
    coordinator.addWrites(pendingWrites);
  }

  // for the future it would be nice if we could reject writes and let the application decide how to proceed
  private void waitForWritesToUnblock() throws IOException {
    LOGGER.warning("There are too many in-flight write jobs. Waiting for them to finish");
    try {
      while (coordinator.hasMaxInFlightWriters() && coordinator.isUsable()) {
        synchronized (coordinator) {
          coordinator.wait();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedExceptionWrapper("interrupted waiting for write jobs to finish");
    }
    lock.runWithWriteLock(this::sendWritesToCoordinator);
  }
}

package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.InterruptedExceptionWrapper;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;
import dbf0.disk_key_value.readwrite.ReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.log.LogConsumer;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import dbf0.document.types.DElement;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class LsmTree<T extends OutputStream, K, V> implements ReadWriteStorage<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTree.class);

  public static class Builder<T extends OutputStream, K, V> {

    private final LsmTreeConfiguration<K, V> configuration;

    private BaseDeltaFiles<T, K, V> baseDeltaFiles;
    private DeltaWriterCoordinator<T, K, V> deltaWriterCoordinator;
    private BaseDeltaMergerCron<T, K, V> mergerCron;
    private ScheduledExecutorService executorService;
    private WriteAheadLog<?> writeAheadLog;


    public Builder(final LsmTreeConfiguration<K, V> configuration) {
      this.configuration = Preconditions.checkNotNull(configuration);
    }


    public Builder<T, K, V> withBaseDeltaFiles(BaseDeltaFiles<T, K, V> baseDeltaFiles) {
      this.baseDeltaFiles = Preconditions.checkNotNull(baseDeltaFiles);
      return this;
    }

    public Builder<T, K, V> withBaseDeltaFiles(FileDirectoryOperations<T> fileDirectoryOperations) {
      return withBaseDeltaFiles(new BaseDeltaFiles<>(
          configuration.getKeySerialization().getDeserializer(),
          configuration.getValueSerialization().getDeserializer(),
          configuration.getKeyComparator(),
          fileDirectoryOperations));
    }

    public Builder<T, K, V> withDeltaWriteCoordinator(DeltaWriterCoordinator<T, K, V> coordinator) {
      this.deltaWriterCoordinator = Preconditions.checkNotNull(coordinator);
      return this;
    }

    public Builder<T, K, V> withMergerCron(BaseDeltaMergerCron<T, K, V> mergerCron) {
      this.mergerCron = Preconditions.checkNotNull(mergerCron);
      return this;
    }

    public Builder<T, K, V> withScheduledExecutorService(ScheduledExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public Builder<T, K, V> withScheduledThreadPool(int corePoolSize) {
      return withScheduledExecutorService(Executors.newScheduledThreadPool(corePoolSize));
    }

    public Builder<T, K, V> withWriteAheadLog(WriteAheadLog<?> writeAheadLog) {
      this.writeAheadLog = writeAheadLog;
      return this;
    }

    public LsmTree<T, K, V> build() {
      return buildInternal().getLeft();
    }

    public ReadWriteStorageWithBackgroundTasks<K, V> buildWithBackgroundTasks() {
      var pair = buildInternal();
      return new ReadWriteStorageWithBackgroundTasks<>(pair.getLeft(), pair.getRight());
    }

    private Pair<LsmTree<T, K, V>, ExecutorService> buildInternal() {
      Preconditions.checkState(baseDeltaFiles != null, "must specify baseDeltaFiles");
      ScheduledExecutorService executorService = this.executorService;
      if (executorService == null) {
        executorService = Executors.newScheduledThreadPool(4);
      }
      DeltaWriterCoordinator<T, K, V> coordinator = this.deltaWriterCoordinator;
      if (coordinator == null) {
        coordinator = new DeltaWriterCoordinator<>(configuration, baseDeltaFiles, executorService, writeAheadLog);
      }
      BaseDeltaMergerCron<T, K, V> mergerCron = this.mergerCron;
      if (mergerCron == null) {
        mergerCron = new BaseDeltaMergerCron<>(configuration, baseDeltaFiles, executorService);
      }
      return Pair.of(new LsmTree<>(configuration, baseDeltaFiles, coordinator, mergerCron, writeAheadLog),
          executorService);
    }
  }

  public static <T extends OutputStream, K, V> Builder<T, K, V> builder(LsmTreeConfiguration<K, V> config) {
    return new Builder<>(config);
  }

  public static <T extends OutputStream> Builder<T, ByteArrayWrapper, ByteArrayWrapper>
  builderForBytes(LsmTreeConfiguration<ByteArrayWrapper, ByteArrayWrapper> config) {
    return builder(config);
  }

  public static <T extends OutputStream> Builder<T, ByteArrayWrapper, ByteArrayWrapper> builderForBytes() {
    return builderForBytes(LsmTreeConfiguration.builderForBytes().build());
  }

  public static <K, V> Builder<FileOutputStream, K, V>
  builderForDirectory(FileDirectoryOperations<FileOutputStream> directoryOperations,
                      LsmTreeConfiguration<K, V> configuration) {
    return LsmTree.<FileOutputStream, K, V>builder(configuration).withBaseDeltaFiles(directoryOperations);
  }

  public static Builder<FileOutputStream, ByteArrayWrapper, ByteArrayWrapper>
  builderForDirectory(FileDirectoryOperations<FileOutputStream> directoryOperations) {
    return LsmTree.<FileOutputStream>builderForBytes().withBaseDeltaFiles(directoryOperations);
  }

  public static <T extends OutputStream> Builder<T, DElement, DElement>
  builderForDocuments(LsmTreeConfiguration<DElement, DElement> configuration) {
    return builder(configuration);
  }

  public static <T extends OutputStream> Builder<T, DElement, DElement> builderForDocuments() {
    return builderForDocuments(LsmTreeConfiguration.builderForDocuments().build());
  }

  public static <K, V> Builder<MemoryFileOperations.MemoryOutputStream, K, V> builderForTesting(
      MemoryFileDirectoryOperations directoryOperations, LsmTreeConfiguration<K, V> configuration) {
    return LsmTree.<MemoryFileOperations.MemoryOutputStream, K, V>builder(configuration)
        .withBaseDeltaFiles(directoryOperations);
  }

  public static Builder<MemoryFileOperations.MemoryOutputStream, ByteArrayWrapper, ByteArrayWrapper> builderForTesting(
      MemoryFileDirectoryOperations directoryOperations) {
    return LsmTree.<MemoryFileOperations.MemoryOutputStream>builderForBytes()
        .withBaseDeltaFiles(directoryOperations);
  }

  private final LsmTreeConfiguration<K, V> configuration;
  private final BaseDeltaFiles<T, K, V> baseDeltaFiles;
  private final DeltaWriterCoordinator<T, K, V> coordinator;
  private final BaseDeltaMergerCron<T, K, V> mergerCron;
  @Nullable private final WriteAheadLog<?> writeAheadLog;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private PendingWritesAndLog<K, V> pendingWrites;

  private LsmTree(LsmTreeConfiguration<K, V> configuration,
                  BaseDeltaFiles<T, K, V> baseDeltaFiles,
                  DeltaWriterCoordinator<T, K, V> coordinator,
                  BaseDeltaMergerCron<T, K, V> mergerCron,
                  @Nullable WriteAheadLog<?> writeAheadLog) {
    this.configuration = configuration;
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
        pendingWrites = new PendingWritesAndLog<>(new HashMap<>(), null);
        var keyDeserializer = configuration.getKeySerialization().getDeserializer();
        var valueDeserializer = configuration.getValueSerialization().getDeserializer();
        return new LogConsumer() {
          @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
            pendingWrites.writes.put(keyDeserializer.deserialize(key), valueDeserializer.deserialize(value));
          }

          @Override public void delete(@NotNull ByteArrayWrapper key) throws IOException {
            pendingWrites.writes.put(keyDeserializer.deserialize(key), configuration.getDeleteValue());
          }

          @Override public void persist() throws IOException {
            sendWritesToCoordinator();
            coordinator.addWrites(pendingWrites);
            pendingWrites = new PendingWritesAndLog<>(new HashMap<>(configuration.getPendingWritesDeltaThreshold()), null);
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
        if (pendingWrites == null) {
          return;
        }
        if (!pendingWrites.writes.isEmpty()) {
          LOGGER.info("writing pending at close");
          if (coordinator.hasMaxInFlightWriters()) {
            LOGGER.warning("Delaying close because DeltaWriteCoordinator is backed up");
            waitForWritesToUnblock();
          } else {
            sendWritesToCoordinator();
          }
        }
        pendingWrites = null;
        try {
          while (coordinator.hasInFlightWriters() && coordinator.isUsable()) {
            LOGGER.info("Waiting for deltas to finish writing");
            coordinator.awaitNextJobCompletion();
          }
        } catch (InterruptedException e) {
          var msg = "Interrupted waiting for deltas to finish writing";
          LOGGER.warning(msg);
          throw new InterruptedExceptionWrapper(msg, e);
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

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (pendingWrites.logWriter != null) {
        pendingWrites.logWriter.logPut(
            configuration.getKeySerialization().getSerializer().serializeToBytes(key),
            configuration.getValueSerialization().getSerializer().serializeToBytes(value));
      }
      pendingWrites.writes.put(key, value);
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
  }

  @Nullable @Override public V get(@NotNull K key) throws IOException {
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

  private @Nullable V checkForDeleteValue(@NotNull V value) {
    return value.equals(configuration.getDeleteValue()) ? null : value;
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (pendingWrites.logWriter != null) {
        pendingWrites.logWriter.logDelete(configuration.getKeySerialization().getSerializer().serializeToBytes(key));
      }
      pendingWrites.writes.put(key, configuration.getDeleteValue());
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
    return true;// doesn't actually return a useful value
  }

  public void waitForAllDeltasToMerge() throws InterruptedException {
    Preconditions.checkState(isUsable());
    mergerCron.waitForAllDeltasToMerge();
  }

  // only to be called when holding the write lock
  private boolean checkMergeThreshold() throws IOException {
    if (pendingWrites.writes.size() < configuration.getPendingWritesDeltaThreshold()) {
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
    pendingWrites = new PendingWritesAndLog<>(new HashMap<>(configuration.getPendingWritesDeltaThreshold()),
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
      boolean exit;
      do {
        while (coordinator.hasMaxInFlightWriters() && coordinator.isUsable()) {
          synchronized (coordinator) {
            coordinator.awaitNextJobCompletion();
          }
        }
        exit = lock.callWithWriteLock(() -> {
          if (!coordinator.isUsable()) {
            return true;
          }
          if (coordinator.hasMaxInFlightWriters()) {
            return false;
          }
          sendWritesToCoordinator();
          createNewPendingWrites();
          return true;
        });
      } while (!exit);
    } catch (InterruptedException e) {
      throw new InterruptedExceptionWrapper("interrupted waiting for write jobs to finish", e);
    }
  }
}

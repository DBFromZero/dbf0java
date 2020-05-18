package dbf0.disk_key_value.readwrite.lsmtree.singlevalue;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.InterruptedExceptionWrapper;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.readonly.singlevalue.RandomAccessKeyValueFileReader;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;
import dbf0.disk_key_value.readwrite.ReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.log.LogConsumer;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.base.BaseDeltaFiles;
import dbf0.disk_key_value.readwrite.lsmtree.base.BaseDeltaMergerCron;
import dbf0.disk_key_value.readwrite.lsmtree.base.PendingWritesAndLog;
import dbf0.disk_key_value.readwrite.lsmtree.base.WriteJobCoordinator;
import dbf0.document.types.DElement;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class LsmTree<T extends OutputStream, K, V> implements ReadWriteStorage<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTree.class);

  public static class Builder<T extends OutputStream, K, V> {

    protected final LsmTreeConfiguration<K, V> configuration;
    protected BaseDeltaFiles<T, K, V, RandomAccessKeyValueFileReader<K, V>> baseDeltaFiles;
    protected ScheduledExecutorService executorService;
    protected WriteAheadLog<?> writeAheadLog;

    public Builder(final LsmTreeConfiguration<K, V> configuration) {
      this.configuration = Preconditions.checkNotNull(configuration);
    }

    public Builder<T, K, V> withBaseDeltaFiles(FileDirectoryOperations<T> fileDirectoryOperations) {
      this.baseDeltaFiles = new BaseDeltaFiles<>(fileDirectoryOperations,
          (baseOperations, indexOperations) -> RandomAccessKeyValueFileReader.open(
              configuration.getKeySerialization().getDeserializer(),
              configuration.getValueSerialization().getDeserializer(),
              configuration.getKeyComparator(),
              baseOperations, indexOperations));
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
      var executorService = this.executorService;
      if (executorService == null) {
        executorService = Executors.newScheduledThreadPool(4);
      }
      var coordinator = new WriteJobCoordinator<>(baseDeltaFiles, executorService, writeAheadLog,
          new SortAndWriteKeyValues<>(configuration), configuration.getMaxInFlightWriteJobs());
      var mergerCron = new BaseDeltaMergerCron<>(baseDeltaFiles, executorService, configuration.getMergeCronFrequency(),
          configuration.getMaxDeltaReadPercentage(), LsmTreeMerger.create(configuration));
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
  private final BaseDeltaFiles<T, K, V, RandomAccessKeyValueFileReader<K, V>> baseDeltaFiles;
  private final WriteJobCoordinator<T, Map<K, V>> coordinator;
  private final BaseDeltaMergerCron<T> mergerCron;
  @Nullable private final WriteAheadLog<?> writeAheadLog;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private PendingWritesAndLog<Map<K, V>> pendingWrites;

  private LsmTree(LsmTreeConfiguration<K, V> configuration,
                  BaseDeltaFiles<T, K, V, RandomAccessKeyValueFileReader<K, V>> baseDeltaFiles,
                  WriteJobCoordinator<T, Map<K, V>> coordinator,
                  BaseDeltaMergerCron<T> mergerCron,
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
            pendingWrites.getWrites().put(keyDeserializer.deserialize(key), valueDeserializer.deserialize(value));
          }

          @Override public void delete(@NotNull ByteArrayWrapper key) throws IOException {
            pendingWrites.getWrites().put(keyDeserializer.deserialize(key), configuration.getDeleteValue());
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
        if (!pendingWrites.getWrites().isEmpty()) {
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
      if (pendingWrites.getLogWriter() != null) {
        pendingWrites.getLogWriter().logPut(
            configuration.getKeySerialization().getSerializer().serializeToBytes(key),
            configuration.getValueSerialization().getSerializer().serializeToBytes(value));
      }
      pendingWrites.getWrites().put(key, value);
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
      return pendingWrites.getWrites().get(key);
    });
    if (value != null) {
      return checkForDeleteValue(value);
    }
    value = searchForKeyInWritesInProgress(key);
    if (value != null) {
      return checkForDeleteValue(value);
    }
    if (!baseDeltaFiles.hasInUseBase()) {
      return null;
    }
    value = searchForKeyInFiles(key);
    return value == null ? null : checkForDeleteValue(value);
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (pendingWrites.getLogWriter() != null) {
        pendingWrites.getLogWriter().logDelete(configuration.getKeySerialization().getSerializer().serializeToBytes(key));
      }
      pendingWrites.getWrites().put(key, configuration.getDeleteValue());
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

  @Nullable private V checkForDeleteValue(@NotNull V value) {
    return value.equals(configuration.getDeleteValue()) ? null : value;
  }

  @Nullable private V searchForKeyInWritesInProgress(K key) {
    var jobs = coordinator.getCurrentInFlightJobs();
    for (int i = jobs.size() - 1; i >= 0; i--) {
      var value = jobs.get(i).getPendingWritesAndLog().getWrites().get(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  @Nullable private V searchForKeyInFiles(K key) throws IOException {
    // prefer the later deltas and finally the base
    var deltaValue = recursivelySearchDeltasInReverse(baseDeltaFiles.getOrderedDeltaReaders().iterator(), key);
    if (deltaValue != null) {
      return deltaValue;
    }
    var baseWrapper = baseDeltaFiles.getBaseWrapper();
    return baseWrapper.getLock().callWithReadLock(() -> Preconditions.checkNotNull(baseWrapper.getReader()).get(key));
  }

  @Nullable private V recursivelySearchDeltasInReverse(
      Iterator<BaseDeltaFiles.ReaderWrapper<K, V, RandomAccessKeyValueFileReader<K, V>>> iterator,
      K key) throws IOException {
    if (!iterator.hasNext()) {
      return null;
    }
    var wrapper = iterator.next();
    var result = recursivelySearchDeltasInReverse(iterator, key);
    if (result != null) {
      return result;
    }
    // note that it is possible the delta was deleted while we were searching other deltas or
    // while we were waiting for the read lock.
    // that is fine because now the delta is in the base and we always search the deltas first
    // so if the key was in the delta then we'll find it when we search the base
    return wrapper.getLock().callWithReadLock(() -> wrapper.getReader() == null ? null : wrapper.getReader().get(key));
  }

  // only to be called when holding the write lock
  private boolean checkMergeThreshold() throws IOException {
    if (pendingWrites.getWrites().size() < configuration.getPendingWritesDeltaThreshold()) {
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
    if (pendingWrites.getLogWriter() != null) {
      pendingWrites.getLogWriter().close();
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

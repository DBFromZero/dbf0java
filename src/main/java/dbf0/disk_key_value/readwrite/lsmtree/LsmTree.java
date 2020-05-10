package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadWriteLockHelper;
import dbf0.common.io.SerializationPair;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.readwrite.CloseableReadWriteStorage;
import dbf0.disk_key_value.readwrite.ReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.log.LogConsumer;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import dbf0.document.types.DElement;
import dbf0.document.types.DString;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class LsmTree<T extends OutputStream, K, V> implements CloseableReadWriteStorage<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTree.class);
  public static final ByteArrayWrapper BYTE_ARRAY_DELETE_VALUE = ByteArrayWrapper.of(
      83, 76, 69, 7, 95, 21, 81, 27, 2, 104, 8, 100, 45, 109, 110, 1);
  public static final DString D_ELEMENT_DELETE_VALUE = new DString("|fxcR/*rwEC\\rMg/^");

  public static class Builder<T extends OutputStream, K, V> {
    private int pendingWritesDeltaThreshold = 10 * 1000;
    private SerializationPair<K> keySerialization;
    private SerializationPair<V> valueSerialization;
    private Comparator<K> keyComparator;
    private V deleteValue;
    private BaseDeltaFiles<T, K, V> baseDeltaFiles;
    private DeltaWriterCoordinator<T, K, V> deltaWriterCoordinator;
    private BaseDeltaMergerCron<T, K, V> mergerCron;
    private ScheduledExecutorService executorService;
    private WriteAheadLog<?> writeAheadLog;


    private int indexRate = 10;
    private int maxInFlightWriteJobs = 10;
    double maxDeltaReadPercentage = 0.5;
    private Duration mergeCronFrequency = Duration.ofSeconds(1);

    public Builder() {
    }

    public Builder<T, K, V> withPendingWritesDeltaThreshold(int pendingWritesDeltaThreshold) {
      Preconditions.checkArgument(pendingWritesDeltaThreshold > 0);
      this.pendingWritesDeltaThreshold = pendingWritesDeltaThreshold;
      return this;
    }

    public Builder<T, K, V> withKeySerialization(SerializationPair<K> keySerialization) {
      this.keySerialization = Preconditions.checkNotNull(keySerialization);
      return this;
    }

    public Builder<T, K, V> withValueSerialization(SerializationPair<V> valueSerialization) {
      this.valueSerialization = Preconditions.checkNotNull(valueSerialization);
      return this;
    }

    public Builder<T, K, V> withKeyComparator(Comparator<K> keyComparator) {
      this.keyComparator = Preconditions.checkNotNull(keyComparator);
      return this;
    }

    public Builder<T, K, V> withDeleteValue(V deleteValue) {
      this.deleteValue = Preconditions.checkNotNull(deleteValue);
      return this;
    }

    public Builder<T, K, V> withBaseDeltaFiles(BaseDeltaFiles<T, K, V> baseDeltaFiles) {
      this.baseDeltaFiles = Preconditions.checkNotNull(baseDeltaFiles);
      return this;
    }

    public Builder<T, K, V> withBaseDeltaFiles(FileDirectoryOperations<T> fileDirectoryOperations) {
      requireKeyValueSerialization();
      return withBaseDeltaFiles(new BaseDeltaFiles<>(
          keySerialization.getDeserializer(), valueSerialization.getDeserializer(), keyComparator, fileDirectoryOperations));
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

    public Builder<T, K, V> withIndexRate(int indexRate) {
      Preconditions.checkArgument(indexRate > 0);
      this.indexRate = indexRate;
      return this;
    }

    public Builder<T, K, V> withMaxInFlightWriteJobs(int maxInFlightWriteJobs) {
      Preconditions.checkArgument(maxInFlightWriteJobs > 0);
      this.maxInFlightWriteJobs = maxInFlightWriteJobs;
      return this;
    }

    public Builder<T, K, V> withMaxDeltaReadPercentage(double maxDeltaReadPercentage) {
      Preconditions.checkArgument(maxDeltaReadPercentage > 0);
      Preconditions.checkArgument(maxDeltaReadPercentage < 1);
      this.maxDeltaReadPercentage = maxDeltaReadPercentage;
      return this;
    }

    public Builder<T, K, V> withMergeCronFrequency(Duration mergeCronFrequency) {
      Preconditions.checkArgument(!mergeCronFrequency.isZero());
      Preconditions.checkArgument(!mergeCronFrequency.isNegative());
      this.mergeCronFrequency = mergeCronFrequency;
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
      Preconditions.checkState(deleteValue != null, "must specify deleteValue");
      requireKeyValueSerialization();
      ScheduledExecutorService executorService = this.executorService;
      if (executorService == null) {
        executorService = Executors.newScheduledThreadPool(4);
      }
      DeltaWriterCoordinator<T, K, V> coordinator = this.deltaWriterCoordinator;
      if (coordinator == null) {
        coordinator = new DeltaWriterCoordinator<T, K, V>(baseDeltaFiles, indexRate, maxInFlightWriteJobs,
            keySerialization.getSerializer(), valueSerialization.getSerializer(), keyComparator,
            executorService, writeAheadLog);
      }
      BaseDeltaMergerCron<T, K, V> mergerCron = this.mergerCron;
      if (mergerCron == null) {
        mergerCron = new BaseDeltaMergerCron<T, K, V>(baseDeltaFiles, keySerialization, valueSerialization, keyComparator,
            deleteValue, maxDeltaReadPercentage, mergeCronFrequency,
            indexRate, executorService);
      }
      return Pair.of(new LsmTree<>(pendingWritesDeltaThreshold, keySerialization, valueSerialization, deleteValue,
          baseDeltaFiles, coordinator, mergerCron, writeAheadLog), executorService);
    }

    private void requireKeyValueSerialization() {
      Preconditions.checkState(keySerialization != null, "must specify keySerialization");
      Preconditions.checkState(valueSerialization != null, "must specify valueSerialization");
      Preconditions.checkState(keyComparator != null, "must specify keyComparator");
    }
  }

  public static <T extends OutputStream, K, V> Builder<T, K, V> builder() {
    return new Builder<>();
  }

  public static <T extends OutputStream> Builder<T, ByteArrayWrapper, ByteArrayWrapper> builderForBytes() {
    return LsmTree.<T, ByteArrayWrapper, ByteArrayWrapper>builder()
        .withKeySerialization(SerializationPair.forByteArrays())
        .withValueSerialization(SerializationPair.forByteArrays())
        .withKeyComparator(ByteArrayWrapper::compareTo)
        .withDeleteValue(BYTE_ARRAY_DELETE_VALUE);
  }

  public static Builder<FileOutputStream, ByteArrayWrapper, ByteArrayWrapper>
  builderForDirectory(FileDirectoryOperations<FileOutputStream> directoryOperations) {
    return LsmTree.<FileOutputStream>builderForBytes()
        .withBaseDeltaFiles(directoryOperations);
  }

  public static <T extends OutputStream> Builder<T, DElement, DElement> builderForDocuments() {
    return LsmTree.<T, DElement, DElement>builder()
        .withKeySerialization(DElement.serializationPair())
        .withValueSerialization(DElement.sizePrefixedSerializationPair())
        .withKeyComparator(DElement::compareTo)
        .withDeleteValue(D_ELEMENT_DELETE_VALUE);
  }

  public static Builder<MemoryFileOperations.MemoryOutputStream, ByteArrayWrapper, ByteArrayWrapper> builderForTesting(
      MemoryFileDirectoryOperations directoryOperations) {
    return LsmTree.<MemoryFileOperations.MemoryOutputStream>builderForBytes()
        .withBaseDeltaFiles(directoryOperations);
  }

  private final int pendingWritesDeltaThreshold;
  private final SerializationPair<K> keySerialization;
  private final SerializationPair<V> valueSerialization;
  private final V deleteValue;
  private final BaseDeltaFiles<T, K, V> baseDeltaFiles;
  private final DeltaWriterCoordinator<T, K, V> coordinator;
  private final BaseDeltaMergerCron<T, K, V> mergerCron;
  @Nullable private final WriteAheadLog<?> writeAheadLog;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private PendingWritesAndLog<K, V> pendingWrites;

  private LsmTree(int pendingWritesDeltaThreshold,
                  SerializationPair<K> keySerialization,
                  SerializationPair<V> valueSerialization,
                  V deleteValue,
                  BaseDeltaFiles<T, K, V> baseDeltaFiles,
                  DeltaWriterCoordinator<T, K, V> coordinator,
                  BaseDeltaMergerCron<T, K, V> mergerCron,
                  @Nullable WriteAheadLog<?> writeAheadLog) {
    this.pendingWritesDeltaThreshold = pendingWritesDeltaThreshold;
    this.keySerialization = keySerialization;
    this.valueSerialization = valueSerialization;
    this.deleteValue = deleteValue;
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
        pendingWrites = new PendingWritesAndLog<>(new HashMap<>(pendingWritesDeltaThreshold), null);
        var keyDeserializer = keySerialization.getDeserializer();
        var valueDeserializer = valueSerialization.getDeserializer();
        return new LogConsumer() {
          @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
            pendingWrites.writes.put(keyDeserializer.deserialize(key), valueDeserializer.deserialize(value));
          }

          @Override public void delete(@NotNull ByteArrayWrapper key) throws IOException {
            pendingWrites.writes.put(keyDeserializer.deserialize(key), deleteValue);
          }

          @Override public void persist() throws IOException {
            sendWritesToCoordinator();
            coordinator.addWrites(pendingWrites);
            pendingWrites = new PendingWritesAndLog<>(new HashMap<>(pendingWritesDeltaThreshold), null);
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

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (pendingWrites.logWriter != null) {
        pendingWrites.logWriter.logPut(keySerialization.getSerializer().serializeToBytes(key),
            valueSerialization.getSerializer().serializeToBytes(value));
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
    return value.equals(deleteValue) ? null : value;
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (pendingWrites.logWriter != null) {
        pendingWrites.logWriter.logDelete(keySerialization.getSerializer().serializeToBytes(key));
      }
      pendingWrites.writes.put(key, deleteValue);
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
    pendingWrites = new PendingWritesAndLog<>(new HashMap<>(pendingWritesDeltaThreshold),
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

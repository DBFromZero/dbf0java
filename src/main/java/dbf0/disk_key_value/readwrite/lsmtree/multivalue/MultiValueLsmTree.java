package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.readonly.multivalue.InMemoryMultiValueResult;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import dbf0.disk_key_value.readonly.multivalue.RandomAccessKeyMultiValueFileReader;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.base.*;
import dbf0.document.types.DElement;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class MultiValueLsmTree<T extends OutputStream, K, V>
    extends BaseLsmTree<T, K, ValueWrapper<V>, PutAndDeletes<K, V>, MultiValuePendingWrites<K, V>,
    RandomAccessKeyMultiValueFileReader<K, ValueWrapper<V>>>
    implements MultiValueReadWriteStorage<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueLsmTree.class);

  public static class Builder<T extends OutputStream, K, V> extends BaseLsmTreeBuilder<T, K, ValueWrapper<V>, Builder<T, K, V>> {

    protected BaseDeltaFiles<T, K, ValueWrapper<V>, RandomAccessKeyMultiValueFileReader<K, ValueWrapper<V>>> baseDeltaFiles;
    private Comparator<V> valueComparator;

    public Builder(final LsmTreeConfiguration<K, ValueWrapper<V>> configuration) {
      super(configuration);
      Preconditions.checkArgument(configuration.getDeleteValue() == null);
    }

    public Builder<T, K, V> withBaseDeltaFiles(FileDirectoryOperations<T> fileDirectoryOperations) {
      this.baseDeltaFiles = new BaseDeltaFiles<>(fileDirectoryOperations,
          (baseOperations, indexOperations) -> RandomAccessKeyMultiValueFileReader.open(
              configuration.getKeySerialization().getDeserializer(),
              configuration.getValueSerialization().getDeserializer(),
              configuration.getKeyComparator(),
              baseOperations, indexOperations));
      return this;
    }

    public Builder<T, K, V> withValueComparator(Comparator<V> valueComparator) {
      this.valueComparator = Preconditions.checkNotNull(valueComparator);
      return this;
    }

    public MultiValueLsmTree<T, K, V> build() {
      return buildInternal().getLeft();
    }

    public MultiValueReadWriteStorageWithBackgroundTasks<K, V> buildWithBackgroundTasks() {
      var pair = buildInternal();
      return new MultiValueReadWriteStorageWithBackgroundTasks<>(pair.getLeft(), pair.getRight());
    }

    private Pair<MultiValueLsmTree<T, K, V>, ExecutorService> buildInternal() {
      Preconditions.checkState(baseDeltaFiles != null, "must specify baseDeltaFiles");
      Preconditions.checkState(valueComparator != null, "must specify valueComparator");
      var executorService = this.executorService;
      if (executorService == null) {
        executorService = Executors.newScheduledThreadPool(4);
      }
      var coordinator = new WriteJobCoordinator<T, PutAndDeletes<K, V>, MultiValuePendingWrites<K, V>>(
          baseDeltaFiles, executorService, null,
          new SortAndWriteKeyMultiValues<>(configuration, valueComparator), configuration.getMaxInFlightWriteJobs());
      var mergerCron = new BaseDeltaMergerCron<>(baseDeltaFiles, executorService, configuration.getMergeCronFrequency(),
          configuration.getMaxDeltaReadPercentage(), new MultiValueLsmTreeMerger<>(configuration, valueComparator));
      return Pair.of(new MultiValueLsmTree<>(configuration, valueComparator, baseDeltaFiles, coordinator, mergerCron),
          executorService);
    }
  }

  public static <T extends OutputStream, K, V> Builder<T, K, V> builder(LsmTreeConfiguration<K, ValueWrapper<V>> config) {
    return new Builder<>(config);
  }

  public static <T extends OutputStream> Builder<T, ByteArrayWrapper, ByteArrayWrapper>
  builderForBytes(LsmTreeConfiguration<ByteArrayWrapper, ValueWrapper<ByteArrayWrapper>> configuration) {
    Builder<T, ByteArrayWrapper, ByteArrayWrapper> b = builder(configuration);
    return b.withValueComparator(ByteArrayWrapper::compareTo);
  }

  public static <T extends OutputStream> Builder<T, ByteArrayWrapper, ByteArrayWrapper>
  builderForBytes(LsmTreeConfiguration<ByteArrayWrapper, ValueWrapper<ByteArrayWrapper>> configuration,
                  FileDirectoryOperations<T> operations) {
    return MultiValueLsmTree.<T>builderForBytes(configuration).withBaseDeltaFiles(operations);
  }

  public static <T extends OutputStream> Builder<T, DElement, DElement>
  builderForDocuments(LsmTreeConfiguration<DElement, ValueWrapper<DElement>> configuration) {
    return builder(configuration);
  }

  public static <T extends OutputStream> Builder<T, ByteArrayWrapper, ByteArrayWrapper> builderForBytes() {
    return builderForBytes(LsmTreeConfiguration.builderForMultiValueBytes().build());
  }

  public static <T extends OutputStream> Builder<T, DElement, DElement> builderForDocuments() {
    Builder<T, DElement, DElement> b = builderForDocuments(
        LsmTreeConfiguration.builderForMultiValueDocuments().build());
    return b.withValueComparator(DElement::compareTo);
  }

  private final Comparator<V> valueComparator;
  private final Comparator<ValueWrapper<V>> valueWrapperComparator;
  private final ThreadLocal<List<MultiValueResult<ValueWrapper<V>>>> localGetList =
      ThreadLocal.withInitial(ArrayList::new);


  private MultiValueLsmTree(LsmTreeConfiguration<K, ValueWrapper<V>> configuration, Comparator<V> valueComparator,
                            BaseDeltaFiles<T, K, ValueWrapper<V>,
                                RandomAccessKeyMultiValueFileReader<K, ValueWrapper<V>>> baseDeltaFiles,
                            WriteJobCoordinator<T, PutAndDeletes<K, V>,
                                MultiValuePendingWrites<K, V>> coordinator,
                            BaseDeltaMergerCron<T> mergerCron) {
    super(configuration, baseDeltaFiles, coordinator, mergerCron);
    this.valueComparator = valueComparator;
    this.valueWrapperComparator = ValueWrapper.comparator(valueComparator);
  }

  @Override public void initialize() throws IOException {
    super.initialize();
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    internalPut(key, value, false);
  }

  @Override public void delete(@NotNull K key, @NotNull V value) throws IOException {
    internalPut(key, value, true);
  }

  private void internalPut(@NotNull K key, @NotNull V value, boolean isDelete) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      if (isDelete) {
        pendingWrites.getWrites().delete(key, value);
      } else {
        pendingWrites.getWrites().put(key, value);
      }
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
  }

  @NotNull public Result<V> get(@NotNull K key) throws IOException {
    Preconditions.checkState(isUsable());

    // ordered newest to oldest
    var orderedResults = localGetList.get();
    orderedResults.clear();

    addSortedInMemoryValues(orderedResults,
        lock.callWithReadLock(() -> {
          Preconditions.checkState(pendingWrites != null, "is closed");
          return pendingWrites.getWrites().getValues(key);
        }));

    var jobs = coordinator.getCurrentInFlightJobs();
    for (int i = jobs.size() - 1; i >= 0; i--) {
      addSortedInMemoryValues(orderedResults, jobs.get(i).getPendingWrites().getWrites().getValues(key));
    }

    MultiValueResult<ValueWrapper<V>> baseResults = null;
    if (baseDeltaFiles.hasInUseBase()) {
      var orderedDeltas = baseDeltaFiles.getOrderedDeltaReaders();
      for (int i = orderedDeltas.size() - 1; i >= 0; i--) {
        var deltaWrapper = orderedDeltas.get(i);
        var deltaResult = deltaWrapper.getLock().callWithReadLock(() ->
            deltaWrapper.getReader() == null ? null : deltaWrapper.getReader().get(key));
        if (deltaResult != null) {
          orderedResults.add(deltaResult);
        }
      }
      var baseWrapper = baseDeltaFiles.getBaseWrapper();
      baseResults = baseWrapper.getLock().callWithReadLock(() -> Preconditions.checkNotNull(baseWrapper.getReader()).get(key));
      if (baseResults != null) {
        orderedResults.add(baseResults);
      }
    }

    if (orderedResults.isEmpty()) {
      return MultiValueReadWriteStorage.emptyResult();
    } else if (orderedResults.size() == 1) {
      return baseResults != null ? new ResultFromBase<>(baseResults) :
          new ResultRemovingDeletesSingleSource<>(orderedResults.get(0));
    } else {
      return new ResultRemovingDeletesMultipleSource<>(orderedResults, valueComparator);
    }
  }

  private void addSortedInMemoryValues(List<MultiValueResult<ValueWrapper<V>>> orderedResults,
                                       List<ValueWrapper<V>> inMemory) {
    if (!inMemory.isEmpty()) {
      if (inMemory.size() > 1) {
        inMemory.sort(valueWrapperComparator);
      }
      orderedResults.add(new InMemoryMultiValueResult<>(inMemory));
    }
  }

  @Override protected MultiValuePendingWrites<K, V> createNewPendingWrites() {
    return new MultiValuePendingWrites<>(new PutAndDeletes<>(configuration.getPendingWritesDeltaThreshold()));
  }

  @Override protected void sendWritesToCoordinator() {
    coordinator.addWrites(pendingWrites);
  }
}

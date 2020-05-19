package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.TreeMultimap;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.readonly.multivalue.InMemoryMultiValueResult;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import dbf0.disk_key_value.readonly.multivalue.RandomAccessKeyMultiValueFileReader;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.base.*;
import dbf0.document.types.DElement;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class MultiValueLsmTree<T extends OutputStream, K, V>
    extends BaseLsmTree<T, K, ValueWrapper<V>, TreeMultimap<K, ValueWrapper<V>>, MultiValuePendingWrites<K, V>,
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

    private Pair<MultiValueLsmTree<T, K, V>, ExecutorService> buildInternal() {
      Preconditions.checkState(baseDeltaFiles != null, "must specify baseDeltaFiles");
      Preconditions.checkState(valueComparator != null, "must specify valueComparator");
      var executorService = this.executorService;
      if (executorService == null) {
        executorService = Executors.newScheduledThreadPool(4);
      }
      var coordinator = new WriteJobCoordinator<T, TreeMultimap<K, ValueWrapper<V>>, MultiValuePendingWrites<K, V>>(
          baseDeltaFiles, executorService, null,
          new SortAndWriteKeyMultiValues<>(configuration), configuration.getMaxInFlightWriteJobs());
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
    return builder(configuration);
  }

  public static <T extends OutputStream> Builder<T, DElement, DElement>
  builderForDocuments(LsmTreeConfiguration<DElement, ValueWrapper<DElement>> configuration) {
    return builder(configuration);
  }

  public static <T extends OutputStream> Builder<T, ByteArrayWrapper, ByteArrayWrapper> builderForBytes() {
    Builder<T, ByteArrayWrapper, ByteArrayWrapper> b = builderForBytes(
        LsmTreeConfiguration.builderForMultiValueBytes().build());
    return b.withValueComparator(ByteArrayWrapper::compareTo);
  }

  public static <T extends OutputStream> Builder<T, DElement, DElement> builderForDocuments() {
    Builder<T, DElement, DElement> b = builderForDocuments(
        LsmTreeConfiguration.builderForMultiValueDocuments().build());
    return b.withValueComparator(DElement::compareTo);
  }

  public static <K, V> Builder<MemoryFileOperations.MemoryOutputStream, K, V> builderForTesting(
      MemoryFileDirectoryOperations directoryOperations, LsmTreeConfiguration<K, ValueWrapper<V>> configuration) {
    return MultiValueLsmTree.<MemoryFileOperations.MemoryOutputStream, K, V>builder(configuration)
        .withBaseDeltaFiles(directoryOperations);
  }

  private final Comparator<V> valueComparator;

  private MultiValueLsmTree(LsmTreeConfiguration<K, ValueWrapper<V>> configuration, Comparator<V> valueComparator,
                            BaseDeltaFiles<T, K, ValueWrapper<V>,
                                RandomAccessKeyMultiValueFileReader<K, ValueWrapper<V>>> baseDeltaFiles,
                            WriteJobCoordinator<T, TreeMultimap<K, ValueWrapper<V>>,
                                MultiValuePendingWrites<K, V>> coordinator,
                            BaseDeltaMergerCron<T> mergerCron) {
    super(configuration, baseDeltaFiles, coordinator, mergerCron);
    this.valueComparator = valueComparator;
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
      pendingWrites.getWrites().put(key, new ValueWrapper<>(isDelete, value));
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
  }

  @Nullable public MultiValueResult<V> get(@NotNull K key) throws IOException {
    Preconditions.checkState(isUsable());
    // search through the various containers that could contain the key in the appropriate order
    var orderedResults = new ArrayList<MultiValueResult<ValueWrapper<V>>>(8);
    lock.runWithReadLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      var pendingVs = pendingWrites.getWrites().get(key);
      if (!pendingVs.isEmpty()) {
        orderedResults.add(new InMemoryMultiValueResult<>(new ArrayList<>(pendingVs)));
      }
    });
    var jobs = coordinator.getCurrentInFlightJobs();
    for (int i = jobs.size() - 1; i >= 0; i--) {
      var inProgressVs = jobs.get(i).getPendingWrites().getWrites().get(key);
      if (!inProgressVs.isEmpty()) {
        orderedResults.add(new InMemoryMultiValueResult<>(inProgressVs));
      }
    }
    if (baseDeltaFiles.hasInUseBase()) {
      addValuesFromStores(key, orderedResults);
    }
    return MultiValueResultFilter.create(orderedResults);
  }

  @Override protected MultiValuePendingWrites<K, V> createNewPendingWrites() {
    return new MultiValuePendingWrites<>(
        TreeMultimap.create(configuration.getKeyComparator(), ValueWrapper.comparator(valueComparator)));
  }

  @Override protected void sendWritesToCoordinator() {
    coordinator.addWrites(pendingWrites);
  }

  private void addValuesFromStores(K key, List<MultiValueResult<ValueWrapper<V>>> orderedResults) throws IOException {
    // prioritize the later deltas and finally the base
    recursivelySearchDeltasInReverse(baseDeltaFiles.getOrderedDeltaReaders().iterator(), key, orderedResults);
    var baseWrapper = baseDeltaFiles.getBaseWrapper();
    var baseResults = baseWrapper.getLock().callWithReadLock(() -> Preconditions.checkNotNull(baseWrapper.getReader()).get(key));
    if (baseResults != null) {
      orderedResults.add(baseResults);
    }
  }

  void recursivelySearchDeltasInReverse(
      Iterator<BaseDeltaFiles.ReaderWrapper<K, ValueWrapper<V>, RandomAccessKeyMultiValueFileReader<K, ValueWrapper<V>>>> iterator,
      K key, List<MultiValueResult<ValueWrapper<V>>> orderedResults) throws IOException {
    if (!iterator.hasNext()) {
      return;
    }
    var wrapper = iterator.next();
    var results = wrapper.getLock().callWithReadLock(() -> wrapper.getReader() == null ? null : wrapper.getReader().get(key));

    recursivelySearchDeltasInReverse(iterator, key, orderedResults);

    if (results != null) {
      orderedResults.add(results);
    }
  }
}

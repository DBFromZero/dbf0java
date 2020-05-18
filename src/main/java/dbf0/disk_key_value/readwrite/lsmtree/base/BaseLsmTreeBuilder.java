package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.readonly.singlevalue.RandomAccessKeyValueFileReader;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;

import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class BaseLsmTreeBuilder<T extends OutputStream, K, V, B extends BaseLsmTreeBuilder<T, K, V, ?>> {
  protected final LsmTreeConfiguration<K, V> configuration;
  protected BaseDeltaFiles<T, K, V, RandomAccessKeyValueFileReader<K, V>> baseDeltaFiles;
  protected ScheduledExecutorService executorService;

  protected BaseLsmTreeBuilder(final LsmTreeConfiguration<K, V> configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  public B withBaseDeltaFiles(FileDirectoryOperations<T> fileDirectoryOperations) {
    this.baseDeltaFiles = new BaseDeltaFiles<>(fileDirectoryOperations,
        (baseOperations, indexOperations) -> RandomAccessKeyValueFileReader.open(
            configuration.getKeySerialization().getDeserializer(),
            configuration.getValueSerialization().getDeserializer(),
            configuration.getKeyComparator(),
            baseOperations, indexOperations));
    return (B) this;
  }

  public B withScheduledExecutorService(ScheduledExecutorService executorService) {
    this.executorService = executorService;
    return (B) this;
  }

  public B withScheduledThreadPool(int corePoolSize) {
    return withScheduledExecutorService(Executors.newScheduledThreadPool(corePoolSize));
  }


}

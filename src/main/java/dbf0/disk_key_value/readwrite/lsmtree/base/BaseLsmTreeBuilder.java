package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;

import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class BaseLsmTreeBuilder<T extends OutputStream, K, V, B extends BaseLsmTreeBuilder<T, K, V, ?>> {
  protected final LsmTreeConfiguration<K, V> configuration;
  protected ScheduledExecutorService executorService;

  protected BaseLsmTreeBuilder(final LsmTreeConfiguration<K, V> configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  public B withScheduledExecutorService(ScheduledExecutorService executorService) {
    this.executorService = executorService;
    return (B) this;
  }

  public B withScheduledThreadPool(int corePoolSize) {
    return withScheduledExecutorService(Executors.newScheduledThreadPool(corePoolSize));
  }


}

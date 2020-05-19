package dbf0.disk_key_value.readwrite;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class MultiValueReadWriteStorageWithBackgroundTasks<K, V> implements MultiValueReadWriteStorage<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueReadWriteStorageWithBackgroundTasks.class);

  private final MultiValueReadWriteStorage<K, V> delegate;
  private final ExecutorService executorService;

  public MultiValueReadWriteStorageWithBackgroundTasks(MultiValueReadWriteStorage<K, V> delegate,
                                                       ExecutorService executorService) {
    this.delegate = delegate;
    this.executorService = executorService;
  }

  @Override public void initialize() throws IOException {
    delegate.initialize();
  }

  @Override public void close() throws IOException {
    delegate.close();
    executorService.shutdown();
    var start = System.nanoTime();
    try {
      var terminated = executorService.awaitTermination(60, TimeUnit.SECONDS);
      Preconditions.checkState(terminated);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    var duration = System.nanoTime() - start;
    LOGGER.info("Executor shutdown took " + Duration.ofNanos(duration));
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    delegate.put(key, value);
  }

  @Nullable @Override public MultiValueResult<V> get(@NotNull K key) throws IOException {
    return delegate.get(key);
  }

  @Override public void delete(@NotNull K key, @NotNull V value) throws IOException {
    delegate.delete(key, value);
  }
}

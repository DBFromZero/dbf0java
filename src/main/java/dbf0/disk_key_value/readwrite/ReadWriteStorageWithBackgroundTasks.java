package dbf0.disk_key_value.readwrite;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ReadWriteStorageWithBackgroundTasks<K, V> implements CloseableReadWriteStorage<K, V> {

  private final CloseableReadWriteStorage<K, V> delegate;
  private final ExecutorService executorService;

  public ReadWriteStorageWithBackgroundTasks(CloseableReadWriteStorage<K, V> delegate,
                                             ExecutorService executorService) {
    this.delegate = delegate;
    this.executorService = executorService;
  }

  @Override public void close() throws IOException {
    delegate.close();
    executorService.shutdown();
    try {
      var terminated = executorService.awaitTermination(60, TimeUnit.SECONDS);
      Preconditions.checkState(terminated);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public long size() throws IOException {
    return delegate.size();
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    delegate.put(key, value);
  }

  @Nullable @Override public V get(@NotNull K key) throws IOException {
    return delegate.get(key);
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    return delegate.delete(key);
  }
}

package dbf0.disk_key_value.readwrite;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

public interface ReadWriteStorage<K, V> extends Closeable {

  default void initialize() throws IOException {
  }

  long size() throws IOException;

  void put(@NotNull K key, @NotNull V value) throws IOException;

  @Nullable V get(@NotNull K key) throws IOException;

  boolean delete(@NotNull K key) throws IOException;

  @Override default void close() throws IOException {
  }
}

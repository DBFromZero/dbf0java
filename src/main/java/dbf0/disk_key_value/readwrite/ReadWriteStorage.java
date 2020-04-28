package dbf0.disk_key_value.readwrite;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;

public interface ReadWriteStorage<K, V> {
  int size() throws IOException;

  void put(@NotNull K key, @NotNull V value) throws IOException;

  @Nullable V get(@NotNull K key) throws IOException;

  boolean delete(@NotNull K key) throws IOException;
}

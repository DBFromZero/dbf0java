package dbf0.disk_key_value.readwrite;

import dbf0.disk_key_value.readonly.multivalue.RandomAccessKeyMultiValueFileReader;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

public interface MultiValueReadWriteStorage<K, V> extends Closeable {

  default void initialize() throws IOException {
  }

  void put(@NotNull K key, @NotNull V value) throws IOException;

  @Nullable RandomAccessKeyMultiValueFileReader.Result<V> get(@NotNull K key) throws IOException;

  void delete(@NotNull K key, @NotNull V value) throws IOException;

  @Override default void close() throws IOException {
  }
}

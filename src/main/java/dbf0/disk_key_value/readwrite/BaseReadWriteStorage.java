package dbf0.disk_key_value.readwrite;

import java.io.Closeable;
import java.io.IOException;

public interface BaseReadWriteStorage<K, V> extends Closeable {

  default void initialize() throws IOException {
  }
}

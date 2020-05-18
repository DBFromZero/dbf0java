package dbf0.disk_key_value.readonly.multivalue;

import dbf0.common.io.IOIterator;

import java.io.Closeable;
import java.io.IOException;

public interface MultiValueResult<V> extends Closeable {
  int count();

  int remaining();

  V readValue() throws IOException;

  IOIterator<V> valueIterator();

  @Override void close() throws IOException;
}

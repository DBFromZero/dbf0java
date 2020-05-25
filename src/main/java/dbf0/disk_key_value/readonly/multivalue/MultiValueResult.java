package dbf0.disk_key_value.readonly.multivalue;

import dbf0.common.io.IOIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface MultiValueResult<V> extends Closeable {
  int count();

  int remaining();

  V readValue() throws IOException;

  IOIterator<V> valueIterator();

  default void skipValue() throws IOException {
    readValue();
  }

  default void skipRemainingValues() throws IOException {
    while (remaining() > 0) {
      skipValue();
    }
  }

  default List<V> realizeRemaining() throws IOException {
    if (remaining() == 0) {
      return List.of();
    }
    var list = new ArrayList<V>(remaining());
    var itr = valueIterator();
    while (itr.hasNext()) {
      list.add(itr.next());
    }
    return list;
  }

  @Override void close() throws IOException;
}

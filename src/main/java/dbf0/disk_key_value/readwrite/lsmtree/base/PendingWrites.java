package dbf0.disk_key_value.readwrite.lsmtree.base;

import java.io.IOException;

public interface PendingWrites<W> {
  W getWrites();

  int size();

  default boolean isEmpty() {
    return size() == 0;
  }

  default void freeWriteAheadLog() throws IOException {
  }
}

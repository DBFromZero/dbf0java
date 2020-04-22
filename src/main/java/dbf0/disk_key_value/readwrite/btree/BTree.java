package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.stream.Stream;

public interface BTree<K extends Comparable<K>, V> {
  int size() throws IOException;

  void put(K key, V value) throws IOException;

  @Nullable V get(K key) throws IOException;

  boolean delete(K key) throws IOException;

  @VisibleForTesting Stream<Long> streamIdsInUse();

  @VisibleForTesting BTreeStorage<K, V> getStorage();
}

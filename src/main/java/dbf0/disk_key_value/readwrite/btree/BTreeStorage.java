package dbf0.disk_key_value.readwrite.btree;

import org.jetbrains.annotations.NotNull;

public interface BTreeStorage<K extends Comparable<K>, V> {

  long NO_ID = -1L;

  long allocateNode();

  void storeNode(long id, @NotNull Node<K, V> node);

  @NotNull Node<K, V> getNode(long id);

  void deleteNode(long id);
}

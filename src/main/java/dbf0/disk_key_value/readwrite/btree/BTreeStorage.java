package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public interface BTreeStorage<K extends Comparable<K>, V> {

  long NO_ID = -1L;

  long allocateNode();

  void storeNode(long id, @NotNull Node<K, V> node);

  @NotNull Node<K, V> getNode(long id);

  void deleteNode(long id);

  @VisibleForTesting Set<Long> getIdsInUse();
}

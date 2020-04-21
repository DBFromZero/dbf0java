package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MemoryBTeeStorage<K extends Comparable<K>, V> implements BTreeStorage<K, V> {

  private long nextId = 0;
  private final Map<Long, Node<K, V>> nodes = new HashMap<>();

  @Override public long allocateNode() {
    return nextId++;
  }

  @Override public void storeNode(@NotNull Node<K, V> node) {
    Preconditions.checkArgument(node.id >= 0);
    nodes.put(node.id, node);
  }

  @Override @NotNull public Node<K, V> getNode(long id) {
    Preconditions.checkArgument(id >= 0);
    var node = nodes.get(id);
    Preconditions.checkArgument(node != null, "no such node id %s", id);
    return node;
  }

  @Override public void deleteNode(long id) {
    Preconditions.checkArgument(id >= 0);
    var node = nodes.remove(id);
    Preconditions.checkArgument(node != null, "no such node id %s", id);
  }

  @Override public Set<Long> getIdsInUse() {
    return Collections.unmodifiableSet(nodes.keySet());
  }
}

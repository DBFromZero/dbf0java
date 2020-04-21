package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.stream.Stream;

public class BTree<K extends Comparable<K>, V> {
  private Node<K, V> root;

  public BTree(int capacity, BTreeStorage<K, V> storage) {
    root = new LeafNode<>(capacity, storage);
  }

  int size() {
    return root.size();
  }

  void put(@NotNull K key, V value) {
    root = root.put(key, value);
  }

  @Nullable V get(@NotNull K key) {
    return root.get(key);
  }

  boolean delete(@NotNull K key) {
    var deleted = root.delete(key);
    if (deleted && root.getCount() == 0) {
      root = new LeafNode<>(root.getCapacity(), root.storage);
    }
    return deleted;
  }

  @VisibleForTesting Stream<Long> streamIdsInUse() {
    return Streams.concat(
        Stream.of(root.id),
        Stream.of(root)
            .filter(ParentNode.class::isInstance)
            .map(ParentNode.class::cast)
            .flatMap(ParentNode::streamIdsInUse)
    );
  }

  @VisibleForTesting BTreeStorage<K, V> getStorage() {
    return root.storage;
  }
}

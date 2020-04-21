package dbf0.disk_key_value.readwrite.btree;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

public class BTree<K extends Comparable<K>, V> {
  private Node<K, V> root;

  public BTree(int capacity) {
    root = new LeafNode<>(capacity);
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
    if (deleted && root instanceof ParentNode && root.getCount() == 0) {
      root = new LeafNode<>(root.getCapacity());
    }
    return deleted;
  }

  Node<K, V> getRoot() {
    return root;
  }

  void compact() {
    if (root instanceof ParentNode) {
      ((ParentNode<K, V>) root).findAdjacentLeavesToCompact();
    }
  }
}

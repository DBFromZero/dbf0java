package dbf0.disk_key_value.readwrite.btree;

import javax.annotation.Nullable;

public class BTree<K extends Comparable<K>, V> {
  private Node<K, V> root;

  public BTree(int capacity) {
    root = new LeafNode<>(capacity);
  }

  int size() {
    return root.size();
  }

  void put(K key, V value) {
    root = root.put(key, value);
  }

  @Nullable V get(K key) {
    return root.get(key);
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

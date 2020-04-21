package dbf0.disk_key_value.readwrite.btree;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

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
    if (deleted && root instanceof ParentNode && root.getCount() == 0) {
      root = new LeafNode<>(root.getCapacity(), root.storage);
    }
    return deleted;
  }

  Node<K, V> getRoot() {
    return root;
  }
}

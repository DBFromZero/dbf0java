package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.stream.Stream;

public class MemoryBTree<K extends Comparable<K>, V> implements BTree<K, V> {
  private final MemoryBTeeStorage<K, V> storage;
  private Node<K, V> root;

  public MemoryBTree(BTreeConfig config) {
    this(new MemoryBTeeStorage<>(config));
  }

  @VisibleForTesting MemoryBTree(MemoryBTeeStorage<K, V> storage) {
    this.storage = storage;
    root = storage.createLeaf();
  }

  @Override public int size() {
    return root.size();
  }

  @Override public void put(@NotNull K key, @NotNull V value) {
    root = root.put(key, value);
  }

  @Override @Nullable public V get(@NotNull K key) {
    return root.get(key);
  }

  @Override public boolean delete(@NotNull K key) {
    var deleted = root.delete(key);
    if (deleted && root.getCount() == 0) {
      root = storage.createLeaf();
    }
    return deleted;
  }

  @Override @VisibleForTesting public Stream<Long> streamIdsInUse() {
    return BTree.streamIdsInUseHelper(root);
  }

  @Override @VisibleForTesting public BTreeStorage<K, V> getStorage() {
    return root.storage;
  }
}

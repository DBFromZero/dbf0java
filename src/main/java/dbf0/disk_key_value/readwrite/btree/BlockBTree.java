package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.stream.Stream;

public class BlockBTree<K extends Comparable<K>, V> implements BTree<K, V> {
  private final BlockBTreeStorage<K, V> storage;
  private Node<K, V> root;

  public BlockBTree(int capacity, BlockBTreeStorage<K, V> storage) {
    root = new LeafNode<>(capacity, storage);
    this.storage = storage;
  }

  @Override public int size() {
    return root.size();
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    try {
      root = root.put(key, value);
    } catch (BlockBTreeStorage.IOExceptionWrapper e) {
      throw new IOException(e);
    }
    storage.writeChanges();
  }

  @Override @Nullable public V get(@NotNull K key) throws IOException {
    try {
      return root.get(key);
    } catch (BlockBTreeStorage.IOExceptionWrapper e) {
      throw new IOException(e);
    }
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    boolean deleted;
    try {
      deleted = root.delete(key);
    } catch (BlockBTreeStorage.IOExceptionWrapper e) {
      throw new IOException(e);
    }
    if (deleted && root.getCount() == 0) {
      root = new LeafNode<>(root.getCapacity(), root.storage);
    }
    if (deleted) {
      storage.writeChanges();
    }
    return deleted;
  }

  @Override @VisibleForTesting public Stream<Long> streamIdsInUse() {
    return BTree.streamIdsInUseHelper(root);
  }

  @Override @VisibleForTesting public BlockBTreeStorage<K, V> getStorage() {
    return (BlockBTreeStorage<K, V>) root.storage;
  }
}

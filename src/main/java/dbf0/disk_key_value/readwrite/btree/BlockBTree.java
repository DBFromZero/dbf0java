package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import dbf0.common.io.IOExceptionWrapper;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.stream.Stream;

public class BlockBTree<K extends Comparable<K>, V> implements BTree<K, V> {
  private final BlockBTreeStorage<K, V> storage;
  private long rootId = BTreeStorage.NO_ID;

  public BlockBTree(BlockBTreeStorage<K, V> storage) {
    this.storage = storage;
  }

  @Override public void initialize() throws IOException {
    Preconditions.checkState(rootId == BTreeStorage.NO_ID, "already initialized");
    var root = storage.createLeaf();
    storage.writeChanges();
    rootId = root.id;
  }

  @Override public void close() throws IOException {
    storage.close();
  }

  private Node<K, V> getRoot() {
    Preconditions.checkState(rootId != BTreeStorage.NO_ID, "not initialized");
    return storage.getNode(rootId);
  }

  @Override public long size() {
    return getRoot().size();
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    storage.startCachingNodes();
    try {
      rootId = getRoot().put(key, value).getId();
    } catch (IOExceptionWrapper e) {
      throw new IOException(e);
    } finally {
      storage.stopCachingNodes();
    }
    storage.writeChanges();
  }

  @Override @Nullable public V get(@NotNull K key) throws IOException {
    try {
      return getRoot().get(key);
    } catch (IOExceptionWrapper e) {
      throw new IOException(e);
    }
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    boolean deleted;
    Node<K, V> root;
    storage.startCachingNodes();
    try {
      root = getRoot();
      deleted = root.delete(key);
    } catch (IOExceptionWrapper e) {
      throw new IOException(e);
    } finally {
      storage.stopCachingNodes();
    }
    if (deleted) {
      if (root.getCount() == 0) {
        root = storage.createLeaf();
        rootId = root.id;
      }
      storage.writeChanges();
    }
    return deleted;
  }

  public void batchPut(Stream<Pair<K, V>> stream) throws IOException {
    storage.startCachingNodes();
    try {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        rootId = getRoot().put(entry.getKey(), entry.getValue()).getId();
      }
    } catch (IOExceptionWrapper e) {
      throw new IOException(e);
    } finally {
      storage.stopCachingNodes();
    }
    storage.writeChanges();
  }

  @Override @VisibleForTesting public Stream<Long> streamIdsInUse() {
    return BTree.streamIdsInUseHelper(getRoot());
  }

  @Override @VisibleForTesting public BlockBTreeStorage<K, V> getStorage() {
    return storage;
  }
}

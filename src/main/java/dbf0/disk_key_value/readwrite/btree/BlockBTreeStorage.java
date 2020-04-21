package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.readwrite.blocks.BlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MetadataStorage;
import dbf0.disk_key_value.readwrite.blocks.SerializationPair;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BlockBTreeStorage<K extends Comparable<K>, V> implements BTreeStorage<K, V> {

  private final MetadataStorage metadataStorage;
  private final BlockStorage blockStorage;

  private long nextId = 0;
  private final Map<Long, Long> nodeIdsToBlockIds = new HashMap<>();
  private final Map<Long, Node<K, V>> nodesToWrite = new HashMap<>();
  private final Set<Long> blocksToDelete = new HashSet<>();

  public BlockBTreeStorage(MetadataStorage metadataStorage,
                           BlockStorage blockStorage,
                           SerializationPair<K> keySerializationPair,
                           SerializationPair<V> valueSerializationPair) {
    this.metadataStorage = metadataStorage;
    this.blockStorage = blockStorage;
    this.keySerializationPair = keySerializationPair;
    this.valueSerializationPair = valueSerializationPair;
  }

  @Override public long allocateNode() {
    return nextId++;
  }

  @Override public void storeNode(@NotNull Node<K, V> node) {
    nodesToWrite.put(node.id, node);
  }

  @Override public @NotNull Node<K, V> getNode(long id) {
    var node = nodesToWrite.get(id);
    if (node != null) {
      return node;
    }
    try {
      return loadNode(id);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void deleteNode(long id) {
    var blockId = nodeIdsToBlockIds.remove(id);
    Preconditions.checkState(blockId != null, "no such node id %s", id);
    blocksToDelete.add(blockId);
  }

  @Override public void nodeChanged(@NotNull Node<K, V> node) {
    storeNode(node);
  }

  @Override public Set<Long> getIdsInUse() {
    return null;
  }

  void writeChanges() throws IOException {
    if (nodesToWrite.isEmpty() && blocksToDelete.isEmpty()) {
      return;
    }
  }

  void load() throws IOException {

  }

  private Node<K, V> loadNode(long nodeId) throws IOException {
    var blockId = nodeIdsToBlockIds.get(nodeId);
    Preconditions.checkState(blockId != null, "no such node id %s", id);
  }

}

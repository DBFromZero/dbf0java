package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.readwrite.blocks.BlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MetadataStorage;
import dbf0.disk_key_value.readwrite.blocks.SerializationHelper;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;

public class BlockBTreeStorage<K extends Comparable<K>, V> implements BTreeStorage<K, V> {

  private final MetadataStorage metadataStorage;
  private final BlockStorage blockStorage;
  private final NodeSerialization<K, V> serialization;

  private long nextId = 0;
  private final Map<Long, Long> nodeIdsToBlockIds = new HashMap<>();

  private final Map<Long, Node<K, V>> nodesToWrite = new HashMap<>();
  private final Set<Long> blocksToFree = new HashSet<>();

  public BlockBTreeStorage(MetadataStorage metadataStorage,
                           BlockStorage blockStorage,
                           NodeSerialization<K, V> serialization) {
    this.metadataStorage = metadataStorage;
    this.blockStorage = blockStorage;
    this.serialization = serialization;
  }

  @Override public long allocateNode() {
    return nextId++;
  }

  @Override public void storeNode(@NotNull Node<K, V> node) {
    System.out.println("store " + node);
    nodesToWrite.put(node.getId(), node);
  }

  @Override public @NotNull Node<K, V> getNode(long id) {
    System.out.println("store get " + id);
    var node = nodesToWrite.get(id);
    if (node != null) {
      return node;
    }
    try {
      return loadNode(id);
    } catch (IOException e) {
      throw new IOExceptionWrapper(e);
    }
  }

  @Override public void deleteNode(long id) {
    System.out.println("store delete " + id);
    if (id == 0) {
      System.out.println("debug");
    }
    var blockId = nodeIdsToBlockIds.remove(id);
    Preconditions.checkState(blockId != null, "no such node id %s", id);
    blocksToFree.add(blockId);
    nodesToWrite.remove(id);
  }

  @Override public void nodeChanged(@NotNull Node<K, V> node) {
    storeNode(node);
  }

  @Override public Set<Long> getIdsInUse() {
    return Collections.unmodifiableSet(nodeIdsToBlockIds.keySet());
  }

  void writeChanges() throws IOException {
    System.out.println("write changes");
    if (!nodesToWrite.isEmpty()) {
      for (var entry : nodesToWrite.entrySet()) {
        var node = entry.getValue();
        Preconditions.checkState(entry.getKey() == node.getId());
        var writer = blockStorage.writeBlock();
        serialization.serialize(writer.serializer(), node);
        writer.commit();
        nodeIdsToBlockIds.put(node.getId(), writer.getBlockId());
      }
      nodesToWrite.clear();
      metadataStorage.updateMetadata(this::updateMetadata);
    }

    for (var blockId : blocksToFree) {
      blockStorage.freeBlock(blockId);
    }
    blocksToFree.clear();
  }

  void load() throws IOException {
    nodeIdsToBlockIds.clear();
    var helper = metadataStorage.readMetadata();
    var count = helper.readInt();
    for (int i = 0; i < count; i++) {
      var nodeId = helper.readLong();
      var blockId = helper.readLong();
      nodeIdsToBlockIds.put(nodeId, blockId);
    }
  }

  private Node<K, V> loadNode(long nodeId) throws IOException {
    var blockId = nodeIdsToBlockIds.get(nodeId);
    Preconditions.checkState(blockId != null, "no such node id %s", nodeId);
    var block = blockStorage.readBlock(blockId);
    return serialization.deserialize(block, this);
  }

  private void updateMetadata(SerializationHelper helper) throws IOException {
    helper.writeInt(nodeIdsToBlockIds.size());
    for (var entry : nodeIdsToBlockIds.entrySet()) {
      helper.writeLong(entry.getKey());
      helper.writeLong(entry.getValue());
    }
  }

  static class IOExceptionWrapper extends RuntimeException {
    public IOExceptionWrapper(IOException cause) {
      super(cause);
    }
  }
}

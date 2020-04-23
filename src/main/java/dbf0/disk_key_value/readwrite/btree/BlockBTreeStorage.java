package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.readwrite.blocks.BlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MetadataStorage;
import dbf0.disk_key_value.readwrite.blocks.SerializationHelper;
import dbf0.disk_key_value.readwrite.blocks.Serializer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class BlockBTreeStorage<K extends Comparable<K>, V> extends BaseBTreeStorage<K, V> {

  private final MetadataStorage metadataStorage;
  private final BlockStorage blockStorage;
  private final NodeSerialization<K, V> serialization;

  private final Map<Long, Long> nodeIdsToBlockIds = new HashMap<>();

  private final Map<Long, Node<K, V>> nodesToWrite = new HashMap<>();
  private final Set<Long> blocksToFree = new HashSet<>();

  // during modifying operations we need to cache nodes in memory so that
  // getNode(id) always returns the same instance
  private boolean cacheNodes = false;
  private final Map<Long, Node<K, V>> getCache = new HashMap<>();

  public BlockBTreeStorage(BTreeConfig config,
                           long nextId,
                           MetadataStorage metadataStorage,
                           BlockStorage blockStorage,
                           NodeSerialization<K, V> serialization) {
    super(config, nextId);
    this.metadataStorage = metadataStorage;
    this.blockStorage = blockStorage;
    this.serialization = serialization;
  }

  public BlockBTreeStorage(BTreeConfig config,
                           MetadataStorage metadataStorage,
                           BlockStorage blockStorage,
                           NodeSerialization<K, V> serialization) {
    super(config);
    this.metadataStorage = metadataStorage;
    this.blockStorage = blockStorage;
    this.serialization = serialization;
  }

  @Override protected void nodeCreated(Node<K, V> node) {
    nodesToWrite.put(node.getId(), node);
  }

  void startCachingNodes() {
    Preconditions.checkState(!cacheNodes);
    cacheNodes = true;
  }

  void stopCachingNodes() {
    Preconditions.checkState(cacheNodes);
    cacheNodes = false;
    getCache.clear();
  }

  @Override public @NotNull Node<K, V> getNode(long id) {
    var node = nodesToWrite.get(id);
    if (node != null) {
      return node;
    }
    if (cacheNodes) {
      node = getCache.get(id);
      if (node != null) {
        return node;
      }
    }
    try {
      node = loadNode(id);
    } catch (IOException e) {
      throw new IOExceptionWrapper(e);
    }
    if (cacheNodes) {
      getCache.put(id, node);
    }
    return node;
  }

  @Override public void deleteNode(long id) {
    var blockId = nodeIdsToBlockIds.remove(id);
    Preconditions.checkState(blockId != null, "no such node id %s", id);
    blocksToFree.add(blockId);
    nodesToWrite.remove(id);
    if (cacheNodes) {
      getCache.remove(id);
    }
  }

  @Override public void nodeChanged(@NotNull Node<K, V> node) {
    Preconditions.checkState(nodesToWrite.containsKey(node.getId()) || nodeIdsToBlockIds.containsKey(node.getId()),
        "no such node %s", node);
    nodesToWrite.put(node.getId(), node);
  }

  @Override public Set<Long> getIdsInUse() {
    return Collections.unmodifiableSet(nodeIdsToBlockIds.keySet());
  }

  void vacuum() throws IOException {
    var blockIdMapping = blockStorage.vacuum();
    var oldBlockIdToNodeId = nodeIdsToBlockIds.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    Preconditions.checkState(oldBlockIdToNodeId.size() == nodeIdsToBlockIds.size());
    Preconditions.checkState(oldBlockIdToNodeId.size() == blockIdMapping.size());

    blockIdMapping.forEach((oldBlockId, newBlockId) -> {
      var nodeId = oldBlockIdToNodeId.get(oldBlockId);
      Preconditions.checkState(nodeId != null);
      var putResult = nodeIdsToBlockIds.put(nodeId, newBlockId);
      Preconditions.checkState(putResult != null && putResult.equals(oldBlockId));
    });
    metadataStorage.updateMetadata(this::updateMetadata);
  }

  void writeChanges() throws IOException {
    Preconditions.checkState(!cacheNodes);

    blockStorage.startBatchWrites();
    if (!nodesToWrite.isEmpty()) {
      for (var entry : nodesToWrite.entrySet()) {
        var node = entry.getValue();
        Preconditions.checkState(entry.getKey() == node.getId());
        var writer = blockStorage.writeBlock();
        serialization.serialize(writer.serializer(), node);
        writer.commit();
        var oldBlockId = nodeIdsToBlockIds.put(node.getId(), writer.getBlockId());
        if (oldBlockId != null) {
          blocksToFree.add(oldBlockId);
        }
      }
      nodesToWrite.clear();
      metadataStorage.updateMetadata(this::updateMetadata);
    }

    for (var blockId : blocksToFree) {
      blockStorage.freeBlock(blockId);
    }
    blocksToFree.clear();
    blockStorage.endBatchWrites();
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
    helper.writeMap(nodeIdsToBlockIds, Serializer.longSerializer(), Serializer.longSerializer());
  }

  static class IOExceptionWrapper extends RuntimeException {
    public IOExceptionWrapper(IOException cause) {
      super(cause);
    }
  }
}

package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;
import dbf0.common.io.IOExceptionWrapper;
import dbf0.disk_key_value.readwrite.blocks.BlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MetadataMap;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BlockBTreeStorage<K extends Comparable<K>, V> extends BaseBTreeStorage<K, V> implements Closeable {

  private final MetadataMap<Long, Long> nodeIdsToBlockIds;
  private final BlockStorage blockStorage;
  private final NodeSerialization<K, V> serialization;


  private final Map<Long, Node<K, V>> nodesToWrite = new HashMap<>();
  private final Set<Long> blocksToFree = new HashSet<>();

  // during modifying operations we need to cache nodes in memory so that
  // getNode(id) always returns the same instance
  private boolean cacheNodes = false;
  private final Map<Long, Node<K, V>> getCache = new HashMap<>();

  public BlockBTreeStorage(BTreeConfig config,
                           long nextId,
                           MetadataMap<Long, Long> nodeIdsToBlockIds,
                           BlockStorage blockStorage,
                           NodeSerialization<K, V> serialization) {
    super(config, nextId);
    this.nodeIdsToBlockIds = nodeIdsToBlockIds;
    this.blockStorage = blockStorage;
    this.serialization = serialization;
  }

  public BlockBTreeStorage(BTreeConfig config,
                           MetadataMap<Long, Long> nodeIdsToBlockIds,
                           BlockStorage blockStorage,
                           NodeSerialization<K, V> serialization) {
    super(config);
    this.nodeIdsToBlockIds = nodeIdsToBlockIds;
    this.blockStorage = blockStorage;
    this.serialization = serialization;
  }

  @Override public void close() throws IOException {
    blockStorage.close();
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
    Long blockId;
    try {
      blockId = nodeIdsToBlockIds.delete(id);
    } catch (IOException e) {
      throw new IOExceptionWrapper(e);
    }
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
    return nodeIdsToBlockIds.unmodifiableMap().keySet();
  }

  public Vacuum vacuum() {
    return new Vacuum(blockStorage.vacuum());
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
    }

    for (var blockId : blocksToFree) {
      blockStorage.freeBlock(blockId);
    }
    blocksToFree.clear();
    blockStorage.endBatchWrites();
  }

  void load() throws IOException {
    throw new RuntimeException("not implemented");
  }

  private Node<K, V> loadNode(long nodeId) throws IOException {
    var blockId = nodeIdsToBlockIds.get(nodeId);
    Preconditions.checkState(blockId != null, "no such node id %s", nodeId);
    var block = blockStorage.readBlock(blockId);
    return serialization.deserialize(block, this);
  }

  public class Vacuum {

    private final BlockStorage.BlockStorageVacuum vacuum;

    public Vacuum(BlockStorage.BlockStorageVacuum vacuum) {
      this.vacuum = vacuum;
    }

    public void writeNewFile() throws IOException {
      vacuum.writeNewFile();
    }

    public void commit() throws IOException {
      var blockIdMapping = vacuum.commit();
      var oldBlockIdToNodeId = nodeIdsToBlockIds.unmodifiableMap().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
      Preconditions.checkState(oldBlockIdToNodeId.size() == nodeIdsToBlockIds.size());
      Preconditions.checkState(oldBlockIdToNodeId.size() == blockIdMapping.size());

      for (var entry : blockIdMapping.entrySet()) {
        var oldBlockId = entry.getKey();
        var newBlockId = entry.getValue();
        var nodeId = oldBlockIdToNodeId.get(oldBlockId);
        Preconditions.checkState(nodeId != null);
        var putResult = nodeIdsToBlockIds.put(nodeId, newBlockId);
        Preconditions.checkState(putResult != null && putResult.equals(oldBlockId));
      }
    }

    public void abort() {
      vacuum.abort();
    }
  }

}

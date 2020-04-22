package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MemoryBlockStorage implements BlockStorage {

  private final Map<Long, ByteArrayWrapper> blocks = new HashMap<>();
  private int nextBlockId;

  private class MemoryBlockWriter implements BlockStorage.BlockWriter {

    private final long blockId;
    private final ByteSerializationHelper serializer = new ByteSerializationHelper();
    private boolean isCommitted = false;

    public MemoryBlockWriter(long blockId) {
      this.blockId = blockId;
    }

    @Override public long getBlockId() {
      return blockId;
    }

    @Override public SerializationHelper serializer() {
      Preconditions.checkState(!isCommitted);
      return serializer;
    }

    @Override public boolean isCommitted() {
      return isCommitted;
    }

    @Override public void commit() {
      Preconditions.checkState(!isCommitted);
      blocks.put(blockId, serializer.getBytes());
      isCommitted = true;
    }
  }

  @Override public BlockWriter writeBlock() throws IOException {
    return new MemoryBlockWriter(nextBlockId++);
  }

  @Override public DeserializationHelper readBlock(long blockId) throws IOException {
    var block = blocks.get(blockId);
    Preconditions.checkArgument(block != null, "No such block id %s", blockId);
    return new DeserializationHelper(block);
  }

  @Override public void freeBlock(long blockId) {
    var block = blocks.remove(blockId);
    Preconditions.checkArgument(block != null, "No such block id %s", blockId);
  }

  @Override public int usedBlockCount() {
    return blocks.size();
  }

  @Override public long totalUsedBytes() {
    return blocks.values().stream().mapToInt(ByteArrayWrapper::length).sum();
  }

  @Override public int unusedBlockCount() {
    return 0;
  }

  @Override public long totalUnusedBytes() {
    return 0;
  }

  @Override public <T> void vacuum(BlockReWriter<T> blockReWriter) {
  }
}

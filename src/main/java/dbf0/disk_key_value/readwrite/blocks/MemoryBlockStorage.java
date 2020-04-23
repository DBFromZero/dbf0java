package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MemoryBlockStorage implements BlockStorage {

  private final Map<Long, ByteArrayWrapper> blocks = new HashMap<>();
  private int nextBlockId = 1000;

  private class MemoryBlockWriter extends BaseBlockWriter<ByteSerializationHelper> {

    public MemoryBlockWriter(long blockId) {
      super(blockId, new ByteSerializationHelper());
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

  @Override public BlockStats getStats() {
    return new BlockStats(
        new BlockCounts(blocks.size(), blocks.values().stream().mapToLong(ByteArrayWrapper::length).sum()),
        new BlockCounts(0, 0)
    );
  }

  @Override public Map<Long, Long> vacuum() throws IOException {
    return blocks.keySet().stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
  }
}

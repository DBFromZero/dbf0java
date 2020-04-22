package dbf0.disk_key_value.readwrite.blocks;

import java.io.IOException;

public interface BlockStorage {

  long NO_SUCH_BLOCK = -1;

  interface BlockWriter {
    long getBlockId();

    SerializationHelper serializer();

    boolean isCommitted();

    void commit() throws IOException;
  }

  BlockWriter writeBlock() throws IOException;

  DeserializationHelper readBlock(long blockId) throws IOException;

  void freeBlock(long blockId) throws IOException;

  int usedBlockCount();

  long totalUsedBytes();

  int unusedBlockCount();

  long totalUnusedBytes();

  interface BlockReWriter<T> {
    // TODO
  }

  <T> void vacuum(BlockReWriter<T> blockReWriter) throws IOException;
}

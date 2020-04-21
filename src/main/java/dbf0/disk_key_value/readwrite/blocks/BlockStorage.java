package dbf0.disk_key_value.readwrite.blocks;

import java.io.IOException;
import java.util.Map;

public interface BlockStorage {

  long NO_SUCH_BLOCK = -1;

  long writeBlock(byte[] bytes) throws IOException;

  byte[] readBlock(long blockId) throws IOException;

  void freeBlock(long blockId) throws IOException;

  int usedBlockCount();

  long totalUsedBytes();

  int unusedBlockCount();

  long totalUnusedBytes();

  interface BlockReWriter<T> {
    Map<Long, T> extractIdsUsedInBlock(byte[] block);

    byte[] rewriteBlockIds(Map<T, Long> newBlockIds);
  }

  <T> void vacuum(BlockReWriter<T> blockReWriter) throws IOException;
}

package dbf0.disk_key_value.readwrite;

import dbf0.common.ByteArrayWrapper;

import java.util.Map;

interface BlockManager {

  long writeBlock(byte[] bytes);

  void freeBlock(long blockId);

  ByteArrayWrapper readBlock(long blockId);

  int usedBlockCount();

  long totalUsedBytes();

  int unusedBlockCount();

  long totalUnusedBytes();

  interface BlockReWriter<T> {

    Map<Long, T> extractIdsUsedInBlock(ByteArrayWrapper block);

    ByteArrayWrapper rewriteBlockIds(Map<T, Long> newBlockIds);
  }

  <T> void vacuum(BlockReWriter<T> blockReWriter);
}

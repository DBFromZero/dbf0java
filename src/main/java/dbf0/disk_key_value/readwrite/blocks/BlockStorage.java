package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface BlockStorage extends Closeable {

  long NO_SUCH_BLOCK = -1;

  interface BlockWriter {
    long getBlockId();

    SerializationHelper serializer();

    boolean isCommitted();

    void commit() throws IOException;
  }

  default void startBatchWrites() throws IOException {
  }

  default void endBatchWrites() throws IOException {
  }

  BlockWriter writeBlock() throws IOException;

  DeserializationHelper readBlock(long blockId) throws IOException;

  void freeBlock(long blockId) throws IOException;

  BlockStats getStats();

  abstract class BaseBlockWriter<T extends SerializationHelper> implements BlockWriter {

    protected final long blockId;
    protected final T serializer;
    protected boolean isCommitted = false;

    protected BaseBlockWriter(long blockId, T serializer) {
      this.blockId = blockId;
      this.serializer = serializer;
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
  }

  BlockStorageVacuum vacuum();

  interface BlockStorageVacuum {

    void writeNewFile() throws IOException;

    Map<Long, Long> commit() throws IOException;

    void abort();
  }
}

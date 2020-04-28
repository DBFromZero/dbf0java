package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public interface IndexBuilder {

  Logger LOGGER = Dbf0Util.getLogger(IndexBuilder.class);

  void accept(long position, ByteArrayWrapper key) throws IOException;

  static IndexBuilder indexBuilder(KeyValueFileWriter storage, int indexRate) {
    return new IndexBuilderImpl(storage, indexRate);
  }

  static IndexBuilder multiIndexBuilder(Iterable<IndexBuilder> indexBuilders) {
    return new MultiIndexBuilder(indexBuilders);
  }

  class IndexBuilderImpl implements IndexBuilder {
    private final KeyValueFileWriter indexStorage;
    private final int indexRate;
    private int keyCount;

    IndexBuilderImpl(KeyValueFileWriter indexStorage, int indexRate) {
      this.indexStorage = Preconditions.checkNotNull(indexStorage);
      Preconditions.checkArgument(indexRate > 0);
      this.indexRate = indexRate;
      this.keyCount = 0;
    }

    @Override public void accept(long position, ByteArrayWrapper key) throws IOException {
      if (keyCount % indexRate == 0) {
        LOGGER.finest(() -> "writing index at " + keyCount);
        indexStorage.append(key, ByteArrayWrapper.of(ByteBuffer.allocate(8).putLong(position).array()));
      }
      keyCount++;
    }
  }

  class MultiIndexBuilder implements IndexBuilder {
    private final ImmutableList<IndexBuilder> indexBuilders;

    public MultiIndexBuilder(Iterable<IndexBuilder> indexBuilders) {
      this.indexBuilders = ImmutableList.copyOf(indexBuilders);
    }

    @Override public void accept(long position, ByteArrayWrapper key) throws IOException {
      for (var indexBuilder : indexBuilders) {
        indexBuilder.accept(position, key);
      }
    }
  }
}

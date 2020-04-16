package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

interface IndexBuilder {

  Logger LOGGER = Dbf0Util.getLogger(IndexBuilder.class);

  void accept(long position, ByteArrayWrapper key) throws IOException;

  static IndexBuilder indexBuilder(BasicKeyValueStorage storage, int indexRate) {
    return new IndexBuilderImpl(storage, indexRate);
  }

  static IndexBuilder multiIndexBuilder(Iterable<IndexBuilder> indexBuilders) {
    return new MultiIndexBuilder(indexBuilders);
  }

  class IndexBuilderImpl implements IndexBuilder {
    private final BasicKeyValueStorage storage;
    private final int indexRate;
    private int currentIndex;

    IndexBuilderImpl(BasicKeyValueStorage storage, int indexRate) {
      this.storage = Preconditions.checkNotNull(storage);
      Preconditions.checkArgument(indexRate > 0);
      this.indexRate = indexRate;
      this.currentIndex = 0;
    }

    @Override
    public void accept(long position, ByteArrayWrapper key) throws IOException {
      if (currentIndex % indexRate == 0) {
        LOGGER.fine(() -> "writing index at " + currentIndex);
        storage.store(key, ByteArrayWrapper.of(ByteBuffer.allocate(8).putLong(position).array()));
      }
      currentIndex++;
    }
  }

  class MultiIndexBuilder implements IndexBuilder {
    private final ImmutableList<IndexBuilder> indexBuilders;

    public MultiIndexBuilder(Iterable<IndexBuilder> indexBuilders) {
      this.indexBuilders = ImmutableList.copyOf(indexBuilders);
    }

    @Override
    public void accept(long position, ByteArrayWrapper key) throws IOException {
      for (var indexBuilder : indexBuilders) {
        indexBuilder.accept(position, key);
      }
    }
  }
}

package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileWriter;

import java.io.IOException;
import java.util.logging.Logger;

public interface IndexBuilder<K> {

  Logger LOGGER = Dbf0Util.getLogger(IndexBuilder.class);

  void accept(long position, K key) throws IOException;

  static <K> IndexBuilder<K> indexBuilder(KeyValueFileWriter<K, Long> storage, int indexRate) {
    return new IndexBuilderImpl<>(storage, indexRate);
  }

  static <K> IndexBuilder<K> multiIndexBuilder(Iterable<IndexBuilder<K>> indexBuilders) {
    return new MultiIndexBuilder<>(indexBuilders);
  }

  class IndexBuilderImpl<K> implements IndexBuilder<K> {
    private final KeyValueFileWriter<K, Long> indexStorage;
    private final int indexRate;
    private int keyCount;

    IndexBuilderImpl(KeyValueFileWriter<K, Long> indexStorage, int indexRate) {
      this.indexStorage = Preconditions.checkNotNull(indexStorage);
      Preconditions.checkArgument(indexRate > 0);
      this.indexRate = indexRate;
      this.keyCount = 0;
    }

    @Override public void accept(long position, K key) throws IOException {
      if (keyCount % indexRate == 0) {
        LOGGER.finest(() -> "writing index at " + keyCount);
        indexStorage.append(key, position);
      }
      keyCount++;
    }
  }

  class MultiIndexBuilder<K> implements IndexBuilder<K> {
    private final ImmutableList<IndexBuilder<K>> indexBuilders;

    public MultiIndexBuilder(Iterable<IndexBuilder<K>> indexBuilders) {
      this.indexBuilders = ImmutableList.copyOf(indexBuilders);
    }

    @Override public void accept(long position, K key) throws IOException {
      for (var indexBuilder : indexBuilders) {
        indexBuilder.accept(position, key);
      }
    }
  }
}

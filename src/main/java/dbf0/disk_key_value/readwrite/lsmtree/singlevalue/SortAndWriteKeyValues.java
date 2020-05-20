package dbf0.disk_key_value.readwrite.lsmtree.singlevalue;

import dbf0.common.Dbf0Util;
import dbf0.common.io.PositionTrackingStream;
import dbf0.common.io.UnsignedLongSerializer;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileWriter;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.base.WriteJob;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

public class SortAndWriteKeyValues<T extends OutputStream, K, V> implements WriteJob.SortAndWriter<T, Map<K, V>> {

  private static final Logger LOGGER = Dbf0Util.getLogger(SortAndWriteKeyValues.class);
  private static final int IO_BUFFER_SIZE = 0x4000;

  private final LsmTreeConfiguration<K, V> configuration;

  public SortAndWriteKeyValues(LsmTreeConfiguration<K, V> configuration) {
    this.configuration = configuration;
  }

  @Override public void sortAndWrite(T dataStream, T indexStream, Map<K, V> writes, boolean isBase) throws IOException {
    LOGGER.info(() -> "Sorting " + writes.size() + " writes");
    var sortedEntries = new ArrayList<>(writes.entrySet());
    sortedEntries.sort(Map.Entry.comparingByKey(configuration.getKeyComparator()));

    try (var outputStream = new PositionTrackingStream(dataStream, IO_BUFFER_SIZE)) {
      var keySerializer = configuration.getKeySerialization().getSerializer();
      try (var indexWriter = new KeyValueFileWriter<>(keySerializer, UnsignedLongSerializer.getInstance(),
          new BufferedOutputStream(indexStream, IO_BUFFER_SIZE))) {
        var indexBuilder = IndexBuilder.indexBuilder(indexWriter, configuration.getIndexRate());
        try (var writer = new KeyValueFileWriter<>(keySerializer,
            configuration.getValueSerialization().getSerializer(), outputStream)) {
          for (var entry : sortedEntries) {
            indexBuilder.accept(outputStream.getPosition(), entry.getKey());
            writer.append(entry.getKey(), entry.getValue());
          }
        }
      }
    }
  }
}

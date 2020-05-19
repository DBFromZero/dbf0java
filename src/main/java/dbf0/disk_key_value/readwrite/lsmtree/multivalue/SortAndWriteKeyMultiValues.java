package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.collect.TreeMultimap;
import dbf0.common.Dbf0Util;
import dbf0.common.io.PositionTrackingStream;
import dbf0.common.io.UnsignedLongSerializer;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileWriter;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileWriter;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.base.WriteJob;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

public class SortAndWriteKeyMultiValues<T extends OutputStream, K, V> implements WriteJob.SortAndWriter<T, TreeMultimap<K, ValueWrapper<V>>> {

  private static final Logger LOGGER = Dbf0Util.getLogger(SortAndWriteKeyMultiValues.class);
  private static final int IO_BUFFER_SIZE = 0x4000;

  private final LsmTreeConfiguration<K, ValueWrapper<V>> configuration;

  public SortAndWriteKeyMultiValues(LsmTreeConfiguration<K, ValueWrapper<V>> configuration) {
    this.configuration = configuration;
  }

  @Override
  public void sortAndWrite(T dataStream, T indexStream, TreeMultimap<K, ValueWrapper<V>> writes) throws IOException {
    LOGGER.info(() -> "Sorting " + writes.size() + " writes");
    try (var outputStream = new PositionTrackingStream(dataStream, IO_BUFFER_SIZE)) {
      var keySerializer = configuration.getKeySerialization().getSerializer();
      try (var indexWriter = new KeyValueFileWriter<>(keySerializer, UnsignedLongSerializer.getInstance(),
          new BufferedOutputStream(indexStream, IO_BUFFER_SIZE))) {
        var indexBuilder = IndexBuilder.indexBuilder(indexWriter, configuration.getIndexRate());
        try (var writer = new KeyMultiValueFileWriter<>(keySerializer,
            configuration.getValueSerialization().getSerializer(), outputStream)) {
          for (var keySet : writes.asMap().entrySet()) {
            var key = keySet.getKey();
            indexBuilder.accept(outputStream.getPosition(), key);
            writer.writeKeysAndValues(key, keySet.getValue());
          }
        }
      }
    }
  }
}

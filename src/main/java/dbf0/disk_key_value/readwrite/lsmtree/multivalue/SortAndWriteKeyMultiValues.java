package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SortAndWriteKeyMultiValues<K, V> implements WriteJob.SortAndWriter<PutAndDeletes<K, V>> {

  private static final Logger LOGGER = Dbf0Util.getLogger(SortAndWriteKeyMultiValues.class);

  private final LsmTreeConfiguration<K, ValueWrapper<V>> configuration;
  private final Comparator<V> valueComparator;

  public SortAndWriteKeyMultiValues(LsmTreeConfiguration<K, ValueWrapper<V>> configuration,
                                    Comparator<V> valueComparator) {
    this.configuration = configuration;
    this.valueComparator = valueComparator;
  }

  @Override
  public void sortAndWrite(PositionTrackingStream outputStream, BufferedOutputStream indexStream,
                           PutAndDeletes<K, V> writes, boolean isBase) throws IOException {
    LOGGER.info(() -> "Sorting " + writes.size() + " writes");
    var entries = writes.getCombined().asMap().entrySet().stream()
        .sorted(Map.Entry.comparingByKey(configuration.getKeyComparator()))
        .collect(Collectors.toList());

    long keyCount = 0, valueCount = 0;
    var wrapperComparator = ValueWrapper.comparator(valueComparator);
    var keySerializer = configuration.getKeySerialization().getSerializer();
    try (var indexWriter = new KeyValueFileWriter<>(keySerializer, UnsignedLongSerializer.getInstance(),
        indexStream)) {
      var indexBuilder = IndexBuilder.indexBuilder(indexWriter, configuration.getIndexRate());
      try (var writer = new KeyMultiValueFileWriter<>(keySerializer,
          configuration.getValueSerialization().getSerializer(), outputStream)) {
        for (var keySet : entries) {
          var key = keySet.getKey();
          indexBuilder.accept(outputStream.getPosition(), key);
          var values = isBase ?
              keySet.getValue().stream().filter(Predicate.not(ValueWrapper::isDelete)).collect(Collectors.toList()) :
              new ArrayList<>(keySet.getValue());
          if (!values.isEmpty()) {
            values.sort(wrapperComparator);
            writer.writeKeysAndValues(key, values);
            keyCount++;
            valueCount += values.size();
          }
        }
      }
    }
    LOGGER.fine("Wrote " + keyCount + " keys and " + valueCount + " values");
  }
}

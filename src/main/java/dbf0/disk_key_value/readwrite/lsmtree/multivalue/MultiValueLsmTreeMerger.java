package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.Dbf0Util;
import dbf0.common.io.PositionTrackingStream;
import dbf0.common.io.UnsignedLongSerializer;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileWriter;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileWriter;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.base.BaseDeltaMergerCron;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MultiValueLsmTreeMerger<K, V> implements BaseDeltaMergerCron.Merger {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueLsmTreeMerger.class);

  private final LsmTreeConfiguration<K, ValueWrapper<V>> configuration;
  private final Comparator<V> valueComparator;

  public MultiValueLsmTreeMerger(LsmTreeConfiguration<K, ValueWrapper<V>> configuration,
                                 Comparator<V> valueComparator) {
    this.configuration = configuration;
    this.valueComparator = valueComparator;
  }

  @Override
  public void merge(List<BufferedInputStream> orderedInputStreams,
                    PositionTrackingStream baseOutputStream,
                    BufferedOutputStream indexOutputStream,
                    BaseDeltaMergerCron.ShutdownChecker shutdownChecker)
      throws IOException, BaseDeltaMergerCron.ShutdownWhileMerging {

    var orderedReaders = orderedInputStreams.stream().map(stream ->
        new KeyMultiValueFileReader<>(configuration.getKeySerialization().getDeserializer(),
            configuration.getValueSerialization().getDeserializer(), stream))
        .collect(Collectors.toList());
    var selectedIterator = MultiValueSelectorIterator.createSortedAndSelectedIterator(
        orderedReaders, configuration.getKeyComparator(), valueComparator);

    try (var writer = new KeyMultiValueFileWriter<>(configuration.getKeySerialization().getSerializer(),
        configuration.getValueSerialization().getSerializer(), baseOutputStream)) {

      try (var indexWriter = new KeyValueFileWriter<>(configuration.getKeySerialization().getSerializer(),
          UnsignedLongSerializer.getInstance(), indexOutputStream)) {
        var indexBuilder = IndexBuilder.indexBuilder(indexWriter, configuration.getIndexRate());

        write(baseOutputStream, shutdownChecker, selectedIterator, writer, indexBuilder);
      }
    }
  }

  private void write(PositionTrackingStream baseOutputStream,
                     BaseDeltaMergerCron.ShutdownChecker shutdownChecker,
                     MultiValueSelectorIterator<K, V> selectedIterator,
                     KeyMultiValueFileWriter<K, ValueWrapper<V>> writer,
                     IndexBuilder<K> indexBuilder) throws IOException, BaseDeltaMergerCron.ShutdownWhileMerging {
    long keyCount = 0, valueCount = 0, lastCheckValueCount = 0;
    while (selectedIterator.hasNext()) {
      if (valueCount - lastCheckValueCount > 50000) {
        if (LOGGER.isLoggable(Level.FINER)) {
          LOGGER.finer("in progress merge: wrote " + keyCount + " keys and " + valueCount + " values with size " +
              Dbf0Util.formatSize(baseOutputStream.getPosition()));
        }
        shutdownChecker.checkShutdown();
        lastCheckValueCount = valueCount;
      }
      var entry = selectedIterator.next();
      if (!entry.getValue().isEmpty()) {
        indexBuilder.accept(baseOutputStream.getPosition(), entry.getKey());
        writer.writeKeysAndValues(entry.getKey(), entry.getValue());
        keyCount++;
        valueCount += entry.getValue().size();
      }
    }
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("wrote " + keyCount + " keys and " + valueCount + " values to new base with size " +
          Dbf0Util.formatSize(baseOutputStream.getPosition()));
    }
  }
}

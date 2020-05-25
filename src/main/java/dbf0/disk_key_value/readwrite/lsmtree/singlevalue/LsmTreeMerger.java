package dbf0.disk_key_value.readwrite.lsmtree.singlevalue;

import dbf0.common.Dbf0Util;
import dbf0.common.io.*;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileReader;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileWriter;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.base.BaseDeltaMergerCron;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class LsmTreeMerger<K, V, X> implements BaseDeltaMergerCron.Merger {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTreeMerger.class);

  private final LsmTreeConfiguration<K, V> configuration;
  private final SerializationPair<X> valueSerialization;
  private final X deleteValue;

  public LsmTreeMerger(LsmTreeConfiguration<K, V> configuration,
                       SerializationPair<X> valueSerialization, X deleteValue) {
    this.configuration = configuration;
    this.valueSerialization = valueSerialization;
    this.deleteValue = deleteValue;
  }

  public static <K, V> LsmTreeMerger<K, V, ?> create(LsmTreeConfiguration<K, V> configuration) {
    var valueSerialization = configuration.getValueSerialization();
    if (valueSerialization.getSerializer().isByteArrayEquivalent()) {
      try {
        // optimization to avoid de-serializing values when the value is size prefixed
        return new LsmTreeMerger<>(configuration, ByteArraySerializer.serializationPair(),
            valueSerialization.getSerializer().serializeToBytes(configuration.getDeleteValue()));
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    } else {
      return new LsmTreeMerger<>(configuration, valueSerialization, configuration.getDeleteValue());
    }
  }

  @Override public void merge(List<BufferedInputStream> orderedInputStreams,
                              PositionTrackingStream baseOutputStream,
                              BufferedOutputStream indexOutputStream,
                              BaseDeltaMergerCron.ShutdownChecker shutdownChecker)
      throws IOException, BaseDeltaMergerCron.ShutdownWhileMerging {
    var orderedReaders = orderedInputStreams.stream().map(stream ->
        new KeyValueFileReader<>(configuration.getKeySerialization().getDeserializer(),
            valueSerialization.getDeserializer(), stream))
        .collect(Collectors.toList());
    var selectedIterator = ValueSelectorIterator.createSortedAndSelectedIterator(
        orderedReaders, configuration.getKeyComparator());

    try (var writer = new KeyValueFileWriter<>(configuration.getKeySerialization().getSerializer(),
        valueSerialization.getSerializer(), baseOutputStream)) {

      try (var indexWriter = new KeyValueFileWriter<>(configuration.getKeySerialization().getSerializer(),
          UnsignedLongSerializer.getInstance(), indexOutputStream)) {
        var indexBuilder = IndexBuilder.indexBuilder(indexWriter, configuration.getIndexRate());

        long i = 0, count = 0;
        while (selectedIterator.hasNext()) {
          if (i++ % 50000 == 0) {
            if (LOGGER.isLoggable(Level.FINER)) {
              LOGGER.finer("writing merged entry " + i + " at " + Dbf0Util.formatSize(baseOutputStream.getPosition()));
            }
            shutdownChecker.checkShutdown();
          }
          var entry = selectedIterator.next();
          if (!entry.getValue().equals(deleteValue)) {
            indexBuilder.accept(baseOutputStream.getPosition(), entry.getKey());
            writer.append(entry.getKey(), entry.getValue());
            count++;
          }
        }
        LOGGER.fine("wrote " + count + " key/value pairs to new base with size " +
            Dbf0Util.formatSize(baseOutputStream.getPosition()));
      }
    }
  }
}

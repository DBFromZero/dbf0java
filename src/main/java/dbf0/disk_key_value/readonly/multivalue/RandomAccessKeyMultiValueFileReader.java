package dbf0.disk_key_value.readonly.multivalue;

import dbf0.common.io.Deserializer;
import dbf0.common.io.IOSupplier;
import dbf0.disk_key_value.io.ReadOnlyFileOperations;
import dbf0.disk_key_value.readonly.base.BaseRandomAccessKeyValueFileReader;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class RandomAccessKeyMultiValueFileReader<K, V> extends
    BaseRandomAccessKeyValueFileReader<K, V, KeyMultiValueFileReader<K, V>> {

  public RandomAccessKeyMultiValueFileReader(TreeMap<K, Long> index,
                                             Comparator<K> keyComparator,
                                             IOSupplier<KeyMultiValueFileReader<K, V>> readerSupplier) {
    super(index, keyComparator, readerSupplier);
  }

  public static <K, V> RandomAccessKeyMultiValueFileReader<K, V> open(Deserializer<K> keyDeserializer,
                                                                      Deserializer<V> valueDeserializer,
                                                                      Comparator<K> keyComparator,
                                                                      ReadOnlyFileOperations fileOperations,
                                                                      ReadOnlyFileOperations indexFileOperations) throws IOException {
    return new RandomAccessKeyMultiValueFileReader<>(
        readIndex(indexIterator(keyDeserializer, indexFileOperations.createInputStream()), keyComparator),
        keyComparator,
        readerSupplier(keyDeserializer, valueDeserializer, fileOperations)
    );
  }

  public static <K, V> IOSupplier<KeyMultiValueFileReader<K, V>> readerSupplier(Deserializer<K> keyDeserializer,
                                                                                Deserializer<V> valueDeserializer,
                                                                                ReadOnlyFileOperations fileOperations) {
    return () ->
        KeyMultiValueFileReader.bufferStream(keyDeserializer, valueDeserializer, fileOperations.createInputStream());
  }


  protected boolean scanForKey(K key, KeyMultiValueFileReader<K, V> reader) throws IOException {
    while (true) {
      var entryKey = reader.readKey();
      if (entryKey == null) {
        return false;
      }
      int cmp = keyComparator.compare(entryKey, key);
      if (cmp == 0) {
        return true;
      } else if (cmp < 0) {
        reader.skipRemainingValues();
      } else {
        return false;
      }
    }
  }

  @Nullable public MultiValueResultImp<V> get(K key) throws IOException {
    try (var reader = readerSupplier.get()) {
      reader.skipBytes(computeSearchStartIndex(key));
      if (scanForKey(key, reader)) {
        return new MultiValueResultImp<>(reader);
      }
      return null;
    }
  }

}

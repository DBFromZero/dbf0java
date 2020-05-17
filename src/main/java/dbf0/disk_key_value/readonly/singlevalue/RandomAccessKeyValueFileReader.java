package dbf0.disk_key_value.readonly.singlevalue;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.ByteArrayDeserializer;
import dbf0.common.io.Deserializer;
import dbf0.common.io.IOSupplier;
import dbf0.disk_key_value.io.ReadOnlyFileOperations;
import dbf0.disk_key_value.io.ReadOnlyFileOperationsImpl;
import dbf0.disk_key_value.readonly.base.BaseRandomAccessKeyValueFileReader;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class RandomAccessKeyValueFileReader<K, V> extends
    BaseRandomAccessKeyValueFileReader<K, V, KeyValueFileReader<K, V>> {

  public RandomAccessKeyValueFileReader(TreeMap<K, Long> index,
                                        Comparator<K> keyComparator,
                                        IOSupplier<KeyValueFileReader<K, V>> readerSupplier) {
    super(index, keyComparator, readerSupplier);
  }

  public static <K, V> RandomAccessKeyValueFileReader<K, V> open(Deserializer<K> keyDeserializer,
                                                                 Deserializer<V> valueDeserializer,
                                                                 Comparator<K> keyComparator,
                                                                 ReadOnlyFileOperations fileOperations,
                                                                 ReadOnlyFileOperations indexFileOperations) throws IOException {
    return new RandomAccessKeyValueFileReader<>(
        readIndex(indexIterator(keyDeserializer, indexFileOperations.createInputStream()), keyComparator),
        keyComparator,
        readerSupplier(keyDeserializer, valueDeserializer, fileOperations)
    );
  }

  public static RandomAccessKeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper>
  openByteArrays(ReadOnlyFileOperations fileOperations, ReadOnlyFileOperations indexFileOperations) throws IOException {
    return open(
        ByteArrayDeserializer.getInstance(),
        ByteArrayDeserializer.getInstance(),
        ByteArrayWrapper::compareTo,
        fileOperations,
        indexFileOperations
    );
  }

  public static RandomAccessKeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper>
  openByteArrays(File file, File indexFile) throws IOException {
    return openByteArrays(new ReadOnlyFileOperationsImpl(file), new ReadOnlyFileOperationsImpl(indexFile));
  }

  public static RandomAccessKeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper>
  openByteArrays(String path, String indexPath) throws IOException {
    return openByteArrays(new File(path), new File(indexPath));
  }

  public static <K, V> IOSupplier<KeyValueFileReader<K, V>> readerSupplier(Deserializer<K> keyDeserializer,
                                                                           Deserializer<V> valueDeserializer,
                                                                           ReadOnlyFileOperations fileOperations) {
    // specifically don't use buffered IO cause we hopefully won't have to read much
    // and we also get to use KeyValueFileReader.skipValue to avoid large segments of data
    // might be worth benchmarking the effect of buffered IO?
    return () -> new KeyValueFileReader<>(keyDeserializer, valueDeserializer, fileOperations.createInputStream());
  }

  @Nullable private V scanForKey(K key, KeyValueFileReader<K, V> reader) throws IOException {
    while (true) {
      var entryKey = reader.readKey();
      if (entryKey == null) {
        return null;
      }
      int cmp = keyComparator.compare(entryKey, key);
      if (cmp == 0) {
        return reader.readValue();
      } else if (cmp < 0) {
        reader.skipValue();
      } else {
        return null;
      }
    }
  }

  @Nullable public V get(K key) throws IOException {
    try (var reader = readerSupplier.get()) {
      reader.skipBytes(computeSearchStartIndex(key));
      return scanForKey(key, reader);
    }
  }
}

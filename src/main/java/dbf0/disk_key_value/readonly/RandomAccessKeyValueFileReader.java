package dbf0.disk_key_value.readonly;

import com.google.common.annotations.VisibleForTesting;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.io.ByteArrayDeserializer;
import dbf0.common.io.Deserializer;
import dbf0.common.io.IoSupplier;
import dbf0.common.io.UnsignedLongDeserializer;
import dbf0.disk_key_value.io.ReadOnlyFileOperations;
import dbf0.disk_key_value.io.ReadOnlyFileOperationsImpl;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class RandomAccessKeyValueFileReader<K, V> {

  private final TreeMap<K, Long> index;
  private final Comparator<K> keyComparator;
  private final IoSupplier<KeyValueFileReader<K, V>> readerSupplier;


  public RandomAccessKeyValueFileReader(TreeMap<K, Long> index,
                                        Comparator<K> keyComparator,
                                        IoSupplier<KeyValueFileReader<K, V>> readerSupplier) {
    this.index = index;
    this.keyComparator = keyComparator;
    this.readerSupplier = readerSupplier;
  }

  public static RandomAccessKeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper>
  forByteArrays(ReadOnlyFileOperations fileOperations, ReadOnlyFileOperations indexFileOperations) throws IOException {
    return new RandomAccessKeyValueFileReader<>(
        readIndex(indexIterator(ByteArrayDeserializer.getInstance(), indexFileOperations.createInputStream())),
        ByteArrayWrapper::compareTo,
        readerSupplier(ByteArrayDeserializer.getInstance(), ByteArrayDeserializer.getInstance(), fileOperations)
    );
  }

  public static RandomAccessKeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper>
  forByteArrays(File file, File indexFile) throws IOException {
    return forByteArrays(new ReadOnlyFileOperationsImpl(file), new ReadOnlyFileOperationsImpl(indexFile));
  }

  public static RandomAccessKeyValueFileReader<ByteArrayWrapper, ByteArrayWrapper>
  forByteArrays(String path, String indexPath) throws IOException {
    return forByteArrays(new File(path), new File(indexPath));
  }

  public static <K, V> IoSupplier<KeyValueFileReader<K, V>> readerSupplier(Deserializer<K> keyDeserializer,
                                                                           Deserializer<V> valueDeserializer,
                                                                           ReadOnlyFileOperations fileOperations) {
    // specifically don't use buffered IO cause we hopefully won't have to read much
    // and we also get to use KeyValueFileReader.skipValue to avoid large segments of data
    // might be worth benchmarking the effect of buffered IO?
    return () -> new KeyValueFileReader<>(keyDeserializer, valueDeserializer, fileOperations.createInputStream());
  }

  public static <K> TreeMap<K, Long> readIndex(Deserializer<K> keyDeserializer,
                                               Comparator<K> keyComparator,
                                               InputStream stream) throws IOException {
    return readIndex(new KeyValueFileIterator<>(indexReader(keyDeserializer, stream)), keyComparator);
  }

  @NotNull
  public static <K> KeyValueFileReader<K, Long> indexReader(Deserializer<K> keyDeserializer, InputStream stream)
      throws IOException {
    return KeyValueFileReader.bufferStream(keyDeserializer, UnsignedLongDeserializer.getInstance(), stream);
  }

  @NotNull
  public static <K> KeyValueFileIterator<K, Long> indexIterator(Deserializer<K> keyDeserializer, InputStream stream)
      throws IOException {
    return new KeyValueFileIterator<>(indexReader(keyDeserializer, stream));
  }

  @NotNull
  public static <K> TreeMap<K, Long> readIndex(KeyValueFileIterator<K, Long> iterator, Comparator<K> keyComparator)
      throws IOException {
    var index = new TreeMap<K, Long>(keyComparator);
    Dbf0Util.iteratorStream(iterator).forEach(
        entry -> index.put(entry.getKey(), entry.getValue()));
    return index;
  }

  @NotNull
  public static <K extends Comparable<K>> TreeMap<K, Long> readIndex(KeyValueFileIterator<K, Long> iterator) throws IOException {
    return readIndex(iterator, null);
  }

  @VisibleForTesting
  @Nullable
  V getForReader(K key, KeyValueFileReader<K, V> reader) throws IOException {
    reader.skipBytes(computeSearchStartIndex(key));
    return scanForKey(key, reader);
  }

  @VisibleForTesting
  long computeSearchStartIndex(K key) {
    return Optional.ofNullable(index.floorEntry(key)).map(Map.Entry::getValue).orElse(0L);
  }

  @VisibleForTesting
  @Nullable
  V scanForKey(K key, KeyValueFileReader<K, V> reader) throws IOException {
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

  @Nullable
  public V get(K key) throws IOException {
    try (var reader = readerSupplier.get()) {
      return getForReader(key, reader);
    }
  }
}

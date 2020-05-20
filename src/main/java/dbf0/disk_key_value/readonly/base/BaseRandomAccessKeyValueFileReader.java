package dbf0.disk_key_value.readonly.base;

import dbf0.common.io.Deserializer;
import dbf0.common.io.IOSupplier;
import dbf0.common.io.UnsignedLongDeserializer;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileIterator;
import dbf0.disk_key_value.readonly.singlevalue.KeyValueFileReader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.TreeMap;

public abstract class BaseRandomAccessKeyValueFileReader<K, V, R extends BaseKeyValueFileReader<K, V>> {

  protected final TreeMap<K, Long> index;
  protected final Comparator<K> keyComparator;
  protected final IOSupplier<R> readerSupplier;


  protected BaseRandomAccessKeyValueFileReader(TreeMap<K, Long> index,
                                               Comparator<K> keyComparator,
                                               IOSupplier<R> readerSupplier) {
    this.index = index;
    this.keyComparator = keyComparator;
    this.readerSupplier = readerSupplier;
  }

  public static <K> TreeMap<K, Long> readIndex(Deserializer<K> keyDeserializer,
                                               Comparator<K> keyComparator,
                                               InputStream stream) throws IOException {
    return readIndex(new KeyValueFileIterator<>(indexReader(keyDeserializer, stream)), keyComparator);
  }

  @NotNull
  public static <K> KeyValueFileReader<K, Long> indexReader(Deserializer<K> keyDeserializer, InputStream stream) {
    return KeyValueFileReader.bufferStream(keyDeserializer, UnsignedLongDeserializer.getInstance(), stream);
  }

  @NotNull
  public static <K> KeyValueFileIterator<K, Long> indexIterator(Deserializer<K> keyDeserializer, InputStream stream) {
    return new KeyValueFileIterator<>(indexReader(keyDeserializer, stream));
  }

  @NotNull
  public static <K> TreeMap<K, Long> readIndex(KeyValueFileIterator<K, Long> iterator, Comparator<K> keyComparator)
      throws IOException {
    var index = new TreeMap<K, Long>(keyComparator);
    iterator.forEachRemaining(entry -> index.put(entry.getKey(), entry.getValue()));
    return index;
  }

  @NotNull
  public static <K extends Comparable<K>> TreeMap<K, Long> readIndex(KeyValueFileIterator<K, Long> iterator)
      throws IOException {
    return readIndex(iterator, null);
  }

  protected long computeSearchStartIndex(K key) {
    var entry = index.floorEntry(key);
    return entry == null ? 0L : entry.getValue();
  }
}

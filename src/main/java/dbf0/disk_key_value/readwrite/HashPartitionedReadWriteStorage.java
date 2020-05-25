package dbf0.disk_key_value.readwrite;

import com.google.common.collect.ImmutableList;
import dbf0.common.io.IOFunction;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.Function;

public class HashPartitionedReadWriteStorage<K, V>
    extends BaseHashPartitionedReadWriteStorage<K, V, ReadWriteStorage<K, V>>
    implements ReadWriteStorage<K, V> {

  public static <K, V> HashPartitionedReadWriteStorage<K, V>
  create(Function<K, Integer> hashFunction, int partitions,
         IOFunction<Integer, ReadWriteStorage<K, V>> factory) throws IOException {
    return new HashPartitionedReadWriteStorage<>(hashFunction,
        BaseHashPartitionedReadWriteStorage.createPartitions(partitions, factory));
  }

  public static <K, V> HashPartitionedReadWriteStorage<K, V>
  create(int partitions, IOFunction<Integer, ReadWriteStorage<K, V>> factory) throws IOException {
    return create(defaultHashFunction(), partitions, factory);
  }

  public HashPartitionedReadWriteStorage(Function<K, Integer> hashFunction,
                                         ImmutableList<ReadWriteStorage<K, V>> partitions) {
    super(hashFunction, partitions);
  }

  public HashPartitionedReadWriteStorage(ImmutableList<ReadWriteStorage<K, V>> partitions) {
    this(defaultHashFunction(), partitions);
  }

  @Override public long size() throws IOException {
    long total = 0;
    for (var partition : partitions) {
      total += partition.size();
    }
    return total;
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    getPartition(key).put(key, value);
  }

  @Nullable @Override public V get(@NotNull K key) throws IOException {
    return getPartition(key).get(key);
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    return getPartition(key).delete(key);
  }
}

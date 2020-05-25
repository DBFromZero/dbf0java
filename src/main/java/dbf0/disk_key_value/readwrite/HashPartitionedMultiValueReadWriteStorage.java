package dbf0.disk_key_value.readwrite;

import com.google.common.collect.ImmutableList;
import dbf0.common.io.IOFunction;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

public class HashPartitionedMultiValueReadWriteStorage<K, V>
    extends BaseHashPartitionedReadWriteStorage<K, V, MultiValueReadWriteStorage<K, V>>
    implements MultiValueReadWriteStorage<K, V> {

  public static <K, V> HashPartitionedMultiValueReadWriteStorage<K, V>
  create(Function<K, Integer> hashFunction, int partitions,
         IOFunction<Integer, MultiValueReadWriteStorage<K, V>> factory) throws IOException {
    return new HashPartitionedMultiValueReadWriteStorage<>(hashFunction,
        BaseHashPartitionedReadWriteStorage.createPartitions(partitions, factory));
  }

  public static <K, V> HashPartitionedMultiValueReadWriteStorage<K, V>
  create(int partitions, IOFunction<Integer, MultiValueReadWriteStorage<K, V>> factory) throws IOException {
    return create(defaultHashFunction(), partitions, factory);
  }

  public HashPartitionedMultiValueReadWriteStorage(Function<K, Integer> hashFunction,
                                                   ImmutableList<MultiValueReadWriteStorage<K, V>> partitions) {
    super(hashFunction, partitions);
  }

  public HashPartitionedMultiValueReadWriteStorage(ImmutableList<MultiValueReadWriteStorage<K, V>> partitions) {
    this(defaultHashFunction(), partitions);
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    getPartition(key).put(key, value);
  }

  @Override public @NotNull Result<V> get(@NotNull K key) throws IOException {
    return getPartition(key).get(key);
  }

  @Override public void delete(@NotNull K key, @NotNull V value) throws IOException {
    getPartition(key).delete(key, value);
  }
}

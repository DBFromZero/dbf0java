package dbf0.disk_key_value.readwrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public class HashPartitionedReadWriteStorage<K, V> implements CloseableReadWriteStorage<K, V> {

  private final Function<K, Integer> hashFunction;
  private final ImmutableList<CloseableReadWriteStorage<K, V>> partitions;

  public static <K> Function<K, Integer> defaultHashFunction() {
    return Objects::hashCode;
  }

  public interface PartitionFactory<K, V> {
    CloseableReadWriteStorage<K, V> create(int partition) throws IOException;
  }

  public static <K, V> HashPartitionedReadWriteStorage<K, V>
  create(Function<K, Integer> hashFunction, int partitions, PartitionFactory<K, V> factory) throws IOException {
    Preconditions.checkArgument(partitions > 0);
    var builder = ImmutableList.<CloseableReadWriteStorage<K, V>>builder();
    for (int i = 0; i < partitions; i++) {
      builder.add(factory.create(i));
    }
    return new HashPartitionedReadWriteStorage<>(hashFunction, builder.build());
  }

  public static <K, V> HashPartitionedReadWriteStorage<K, V>
  create(int partitions, PartitionFactory<K, V> factory) throws IOException {
    return create(defaultHashFunction(), partitions, factory);
  }

  public HashPartitionedReadWriteStorage(Function<K, Integer> hashFunction,
                                         ImmutableList<CloseableReadWriteStorage<K, V>> partitions) {
    Preconditions.checkArgument(partitions.size() > 0);
    this.hashFunction = hashFunction;
    this.partitions = partitions;
  }

  public HashPartitionedReadWriteStorage(ImmutableList<CloseableReadWriteStorage<K, V>> partitions) {
    this(defaultHashFunction(), partitions);
  }

  @Override public void initialize() throws IOException {
    for (var partition : partitions) {
      partition.initialize();
    }
  }

  @Override public void close() throws IOException {
    for (var partition : partitions) {
      partition.close();
    }
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

  private ReadWriteStorage<K, V> getPartition(@NotNull K key) {
    return partitions.get(Math.abs(hashFunction.apply(key)) % partitions.size());
  }
}

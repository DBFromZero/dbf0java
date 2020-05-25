package dbf0.disk_key_value.readwrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import dbf0.common.io.IOFunction;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public abstract class BaseHashPartitionedReadWriteStorage<K, V, S extends BaseReadWriteStorage<K, V>>
    implements BaseReadWriteStorage<K, V> {

  protected final Function<K, Integer> hashFunction;
  protected final ImmutableList<S> partitions;

  public static <K> Function<K, Integer> defaultHashFunction() {
    return Objects::hashCode;
  }

  public static <S> ImmutableList<S>
  createPartitions(int partitions, IOFunction<Integer, S> factory) throws IOException {
    Preconditions.checkArgument(partitions > 0);
    var builder = ImmutableList.<S>builder();
    for (int i = 0; i < partitions; i++) {
      builder.add(factory.apply(i));
    }
    return builder.build();
  }

  protected BaseHashPartitionedReadWriteStorage(Function<K, Integer> hashFunction,
                                                ImmutableList<S> partitions) {
    Preconditions.checkArgument(partitions.size() > 0);
    this.hashFunction = hashFunction;
    this.partitions = partitions;
  }

  protected BaseHashPartitionedReadWriteStorage(ImmutableList<S> partitions) {
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

  protected S getPartition(@NotNull K key) {
    return partitions.get(Math.abs(hashFunction.apply(key)) % partitions.size());
  }
}

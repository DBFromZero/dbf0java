package dbf0.disk_key_value.readwrite;

import dbf0.common.io.IOIterator;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public interface MultiValueReadWriteStorage<K, V> extends Closeable {

  default void initialize() throws IOException {
  }

  void put(@NotNull K key, @NotNull V value) throws IOException;

  @NotNull Result<V> get(@NotNull K key) throws IOException;

  void delete(@NotNull K key, @NotNull V value) throws IOException;

  int UNKNOWN_SIZE = -1;

  interface Result<V> extends Closeable {

    IOIterator<V> iterator();

    default int knownSize() {
      return UNKNOWN_SIZE;
    }

    default int maxSize() {
      return UNKNOWN_SIZE;
    }

    default List<V> realizeRemainingValues() throws IOException {
      var size = knownSize();
      if (size == UNKNOWN_SIZE) {
        size = maxSize();
        if (size == UNKNOWN_SIZE) {
          size = 16;
        }
      }
      var list = new ArrayList<V>(size);
      var iterator = iterator();
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      return list;
    }
  }


  Result EMPTY_RESULT = ((Supplier<Result>) () -> {
    var emptyList = List.of();
    var emptyIterator = IOIterator.of(emptyList.iterator());
    return new Result() {
      @Override public IOIterator<Object> iterator() {
        return emptyIterator;
      }

      @Override public void close() throws IOException {
      }

      @Override public int knownSize() {
        return 0;
      }

      @Override public int maxSize() {
        return 0;
      }

      @Override public List realizeRemainingValues() throws IOException {
        return emptyList;
      }
    };
  }).get();

  static <V> Result<V> emptyResult() {
    return (Result<V>) EMPTY_RESULT;
  }
}

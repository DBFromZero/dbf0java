package dbf0.disk_key_value.readwrite;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.test.KeySetSize;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadWriteStorageTester<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(ReadWriteStorageTester.class);

  public interface Callback<T> {
    void accept(T t) throws IOException;
  }

  public static class Builder<K, V> {
    private final Adapter<K, V> adapter;
    private Supplier<K> knownKeySupplier;
    private Supplier<K> unknownKeySupplier;
    private Supplier<V> valueSupplier;
    private Callback<ReadWriteStorage<K, V>> iterationCallback;
    private Random random;
    private boolean debug = false;
    private boolean checkSize = true;
    private boolean checkDeleteReturnValue = true;

    private Builder(Adapter<K, V> adapter) {
      this.adapter = adapter;
    }

    public Builder<K, V> knownKeySupplier(Supplier<K> knownKeySupplier) {
      this.knownKeySupplier = knownKeySupplier;
      return this;
    }

    public Builder<K, V> unknownKeySupplier(Supplier<K> unknownKeySupplier) {
      this.unknownKeySupplier = unknownKeySupplier;
      return this;
    }

    public Builder<K, V> valueSupplier(Supplier<V> valueSupplier) {
      this.valueSupplier = valueSupplier;
      return this;
    }

    public Builder<K, V> iterationCallback(Callback<ReadWriteStorage<K, V>> iterationCallback) {
      this.iterationCallback = iterationCallback;
      return this;
    }

    public Builder<K, V> random(Random random) {
      this.random = random;
      return this;
    }

    public Builder<K, V> debug(boolean debug) {
      this.debug = debug;
      return this;
    }

    public Builder<K, V> checkSize(boolean checkSize) {
      this.checkSize = checkSize;
      return this;
    }

    public Builder<K, V> checkDeleteReturnValue(boolean checkDeleteReturnValue) {
      this.checkDeleteReturnValue = checkDeleteReturnValue;
      return this;
    }

    public ReadWriteStorageTester<K, V> build() {
      return new ReadWriteStorageTester<>(this);
    }
  }

  public static <K, V> Builder<K, V> builder(ReadWriteStorage<K, V> storage) {
    return new Builder<>(new Adapter<>(storage));
  }

  public static Builder<Integer, Integer> builderForIntegers(ReadWriteStorage<Integer, Integer> storage,
                                                             Random random,
                                                             int keySize) {
    return builder(storage)
        .knownKeySupplier(() -> random.nextInt(keySize))
        .unknownKeySupplier(() -> keySize + random.nextInt(keySize))
        .valueSupplier(random::nextInt)
        .random(random);
  }

  public static Builder<Integer, Integer> builderForIntegers(ReadWriteStorage<Integer, Integer> storage,
                                                             RandomSeed seed,
                                                             KeySetSize keySetSize) {
    return builderForIntegers(storage, seed.random(), keySetSize.size);
  }

  public static Builder<ByteArrayWrapper, ByteArrayWrapper> builderForBytes(
      ReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> storage,
      Random random,
      int keySize,
      int valueSize) {
    return builder(storage)
        .knownKeySupplier(() -> ByteArrayWrapper.random(random, keySize))
        .unknownKeySupplier(() -> ByteArrayWrapper.random(random, keySize + 1))
        .valueSupplier(() -> ByteArrayWrapper.random(random, valueSize))
        .random(random);
  }

  private final Adapter<K, V> adapter;
  private final Supplier<K> knownKeySupplier;
  private final Supplier<K> unknownKeySupplier;
  private final Supplier<V> valueSupplier;
  private final Callback<ReadWriteStorage<K, V>> iterationCallback;
  private final Random random;
  private final boolean debug;
  private final boolean checkSize;
  private final boolean checkDeleteReturnValue;

  private ReadWriteStorageTester(Builder<K, V> builder) {
    this.adapter = builder.adapter;
    this.knownKeySupplier = Preconditions.checkNotNull(builder.knownKeySupplier);
    this.unknownKeySupplier = Preconditions.checkNotNull(builder.unknownKeySupplier);
    this.valueSupplier = Preconditions.checkNotNull(builder.valueSupplier);
    this.iterationCallback = Optional.ofNullable(builder.iterationCallback).orElse((ignored) -> {
    });
    this.random = Optional.ofNullable(builder.random).orElseGet(Random::new);
    this.debug = builder.debug;
    this.checkSize = builder.checkSize;
    this.checkDeleteReturnValue = builder.checkDeleteReturnValue;
  }


  public void testAddDeleteMany(int count) {
    Map<K, V> map = new HashMap<>(count);
    IntStream.range(0, count).forEach(ignored -> {
      doPut(map, knownKeySupplier.get());
      if (checkSize) {
        assertThat(adapter.size()).isEqualTo(map.size());
      }
      runCallback(iterationCallback, adapter.storage);
    });

    map.forEach((key, value) -> assertThat(adapter.get(key)).describedAs("key=%s", key).isEqualTo(value));

    Stream.generate(unknownKeySupplier).limit(count)
        .forEach(key -> assertThat(adapter.get(key)).isNull());

    Stream.generate(unknownKeySupplier).limit(count).forEach(key -> doDelete(null, false, key));

    map.keySet().forEach(key -> doDelete(null, true, key));

    if (checkSize) {
      assertThat(adapter.size()).isEqualTo(0);
    }
  }

  public void testPutDeleteGet(int count, PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate) {
    Map<K, V> map = new HashMap<>(count);
    Supplier<K> existingKeySupplier = () ->
        map.keySet().stream().skip(random.nextInt(map.size())).findAny().get();

    IntStream.range(0, count).forEach(ignored -> {
      var known = (!map.isEmpty()) && random.nextDouble() < knownKeyRate.rate;
      var r = random.nextDouble();
      if (r < putDeleteGet.putRate) {
        var key = known ? existingKeySupplier.get() : knownKeySupplier.get();
        Preconditions.checkState(!known || map.containsKey(key));
        doPut(map, key);
      } else {
        var key = known ? existingKeySupplier.get() : unknownKeySupplier.get();
        Preconditions.checkState(known == map.containsKey(key));
        r -= putDeleteGet.putRate;
        if (r < putDeleteGet.getRate) {
          doGet(map, known, key);
        } else {
          doDelete(map, known, key);
        }
      }
      if (checkSize) {
        assertThat(adapter.size()).isEqualTo(map.size());
      }
      runCallback(iterationCallback, adapter.storage);
    });
  }

  private void doDelete(@Nullable Map<K, V> map, boolean known, K key) {
    print("del " + key);
    var a = assertThat(adapter.delete(key)).describedAs("key=%s", key);
    if (known) {
      if (map != null) {
        map.remove(key);
      }
      if (checkDeleteReturnValue) {
        a.isTrue();
      }
    } else {
      if (checkDeleteReturnValue) {
        a.isFalse();
      }
    }
  }

  private void doGet(Map<K, V> map, boolean known, K key) {
    print("get " + key);
    var a = assertThat(adapter.get(key)).describedAs("key=%s", key);
    if (known) {
      a.isEqualTo(map.get(key));
    } else {
      a.isNull();
    }
  }

  private void doPut(Map<K, V> map, K key) {
    var value = valueSupplier.get();
    print(() -> "put " + key + "=" + value);
    map.put(key, value);
    adapter.put(key, value);
  }

  private void print(Object... args) {
    if (debug) {
      LOGGER.fine(() -> join(args));
    }
  }

  private void print(Supplier<Object> supplier) {
    if (debug) {
      LOGGER.fine(() -> supplier.get().toString());
    }
  }

  @NotNull private String join(Object... args) {
    return Joiner.on(" ").join(args);
  }

  static <T> void runCallback(Callback<T> callback, T x) {
    try {
      callback.accept(x);
    } catch (IOException e) {
      throw new AssertionError(
          Strings.lenientFormat("unexpected IOError in callback"), e);
    }
  }

  private static class Adapter<K, V> {
    private final ReadWriteStorage<K, V> storage;

    private Adapter(ReadWriteStorage<K, V> storage) {
      this.storage = Preconditions.checkNotNull(storage);
    }

    private int size() {
      try {
        return storage.size();
      } catch (IOException e) {
        throw new AssertionError(Strings.lenientFormat("unexpected IOError in size() of %s", storage), e);
      }
    }

    private void put(K key, V value) {
      try {
        storage.put(key, value);
      } catch (IOException e) {
        throw new AssertionError(
            Strings.lenientFormat("unexpected IOError for put() %s=%s of %s", key, value, storage), e);
      }
    }

    @Nullable V get(K key) {
      try {
        return storage.get(key);
      } catch (IOException e) {
        throw new AssertionError(
            Strings.lenientFormat("unexpected IOError for get() %s of %s", key, storage), e);
      }
    }

    boolean delete(K key) {
      try {
        return storage.delete(key);
      } catch (IOException e) {
        throw new AssertionError(
            Strings.lenientFormat("unexpected IOError for delete() %s of %s", key, storage), e);
      }
    }
  }
}

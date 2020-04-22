package dbf0.disk_key_value.readwrite;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadWriteStorageTester<K, V> {

  public static class Builder<K, V> {
    private final Adapter<K, V> adapter;
    private Supplier<K> knownKeySupplier;
    private Supplier<K> unknownKeySupplier;
    private Supplier<V> valueSupplier;
    private Consumer<ReadWriteStorage<K, V>> iterationCallback;
    private Random random;
    private boolean debug = false;

    private Builder(Adapter<K, V> adapter) {
      this.adapter = adapter;
    }

    public Builder<K, V> setKnownKeySupplier(Supplier<K> knownKeySupplier) {
      this.knownKeySupplier = knownKeySupplier;
      return this;
    }

    public Builder<K, V> setUnknownKeySupplier(Supplier<K> unknownKeySupplier) {
      this.unknownKeySupplier = unknownKeySupplier;
      return this;
    }

    public Builder<K, V> setValueSupplier(Supplier<V> valueSupplier) {
      this.valueSupplier = valueSupplier;
      return this;
    }

    public Builder<K, V> setIterationCallback(Consumer<ReadWriteStorage<K, V>> iterationCallback) {
      this.iterationCallback = iterationCallback;
      return this;
    }

    public Builder<K, V> setRandom(Random random) {
      this.random = random;
      return this;
    }

    public Builder<K, V> setDebug(boolean debug) {
      this.debug = debug;
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
        .setKnownKeySupplier(() -> random.nextInt(keySize))
        .setUnknownKeySupplier(() -> keySize + random.nextInt(keySize))
        .setValueSupplier(random::nextInt)
        .setRandom(random);
  }

  public static Builder<Integer, Integer> builderForIntegers(ReadWriteStorage<Integer, Integer> storage,
                                                             RandomSeed seed,
                                                             KeySetSize keySetSize) {
    return builderForIntegers(storage, seed.random(), keySetSize.size);
  }

  private final Adapter<K, V> adapter;
  private final Supplier<K> knownKeySupplier;
  private final Supplier<K> unknownKeySupplier;
  private final Supplier<V> valueSupplier;
  private final Consumer<ReadWriteStorage<K, V>> iterationCallback;
  private final Random random;
  private final boolean debug;

  private ReadWriteStorageTester(Builder<K, V> builder) {
    this.adapter = builder.adapter;
    this.knownKeySupplier = Preconditions.checkNotNull(builder.knownKeySupplier);
    this.unknownKeySupplier = Preconditions.checkNotNull(builder.unknownKeySupplier);
    this.valueSupplier = Preconditions.checkNotNull(builder.valueSupplier);
    this.iterationCallback = Optional.ofNullable(builder.iterationCallback).orElse((ignored) -> {
    });
    this.random = Optional.ofNullable(builder.random).orElseGet(Random::new);
    this.debug = builder.debug;
  }


  public void testAddDeleteMany(int count) {
    Map<K, V> map = new HashMap<>(count);
    IntStream.range(0, count).forEach(ignored -> {
      doPut(map, knownKeySupplier.get());
      assertThat(adapter.size()).isEqualTo(map.size());
      iterationCallback.accept(adapter.storage);
    });

    map.forEach((key, value) -> assertThat(adapter.get(key)).describedAs("key=%s", key).isEqualTo(value));

    Stream.generate(unknownKeySupplier).limit(count)
        .forEach(key -> assertThat(adapter.get(key)).isNull());

    Stream.generate(unknownKeySupplier).limit(count)
        .forEach(key -> assertThat(adapter.delete(key)).isFalse());

    map.keySet().forEach(key -> doDelete(null, true, key));

    assertThat(adapter.size()).isEqualTo(0);
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
      assertThat(adapter.size()).isEqualTo(map.size());
      iterationCallback.accept(adapter.storage);
    });
  }

  public void doDelete(@Nullable Map<K, V> map, boolean known, K key) {
    print("del " + key);
    var a = assertThat(adapter.delete(key)).describedAs("key=%s", key);
    if (known) {
      if (map != null) {
        map.remove(key);
      }
      a.isTrue();
    } else {
      a.isFalse();
    }
  }

  public void doGet(Map<K, V> map, boolean known, K key) {
    print("get " + key);
    var a = assertThat(adapter.get(key)).describedAs("key=%s", key);
    if (known) {
      a.isEqualTo(map.get(key));
    } else {
      a.isNull();
    }
  }

  public void doPut(Map<K, V> map, K key) {
    var value = valueSupplier.get();
    print(() -> "put " + key + "=" + value);
    map.put(key, value);
    adapter.put(key, value);
  }

  private void print(Object... args) {
    if (debug) {
      System.out.println(join(args));
    }
  }

  @NotNull private String join(Object... args) {
    return Joiner.on(" ").join(args);
  }

  private void print(Supplier<Object> supplier) {
    if (debug) {
      System.out.println(supplier.get());
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

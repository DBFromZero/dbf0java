package dbf0.disk_key_value.readwrite;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.test.KeySetSize;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiValueReadWriteStorageTester<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueReadWriteStorageTester.class);

  public interface Callback<T> {
    void accept(T t) throws IOException;
  }

  public static class Builder<K, V> {
    private final Adapter<K, V> adapter;
    private Supplier<K> knownKeySupplier;
    private Supplier<K> unknownKeySupplier;
    private Supplier<V> valueSupplier;
    private Callback<MultiValueReadWriteStorage<K, V>> iterationCallback;
    private Random random;
    private boolean debug = false;

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

    public Builder<K, V> iterationCallback(Callback<MultiValueReadWriteStorage<K, V>> iterationCallback) {
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

    public MultiValueReadWriteStorageTester<K, V> build() {
      return new MultiValueReadWriteStorageTester<>(this);
    }
  }

  public static <K, V> Builder<K, V> builder(MultiValueReadWriteStorage<K, V> storage) {
    return new Builder<>(new Adapter<>(storage));
  }

  public static Builder<Integer, Integer> builderForIntegers(MultiValueReadWriteStorage<Integer, Integer> storage,
                                                             Random random,
                                                             int keySize) {
    return builder(storage)
        .knownKeySupplier(() -> random.nextInt(keySize))
        .unknownKeySupplier(() -> keySize + random.nextInt(keySize))
        .valueSupplier(random::nextInt)
        .random(random);
  }

  public static Builder<Integer, Integer> builderForIntegers(MultiValueReadWriteStorage<Integer, Integer> storage,
                                                             RandomSeed seed,
                                                             KeySetSize keySetSize) {
    return builderForIntegers(storage, seed.random(), keySetSize.size);
  }

  public static Builder<ByteArrayWrapper, ByteArrayWrapper> builderForBytes(
      MultiValueReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> storage,
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
  private final Callback<MultiValueReadWriteStorage<K, V>> iterationCallback;
  private final Random random;
  private final boolean debug;

  private MultiValueReadWriteStorageTester(Builder<K, V> builder) {
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
    SetMultimap<K, V> map = HashMultimap.create();
    IntStream.range(0, count).forEach(ignored -> {
      doPut(map, knownKeySupplier.get());
      runCallback(iterationCallback, adapter.storage);
    });

    map.asMap().forEach((key, value) -> assertThat(adapter.get(key))
        .describedAs("key=%s", key).containsExactlyElementsOf(value));

    Stream.generate(unknownKeySupplier).limit(count)
        .forEach(key -> assertThat(adapter.get(key)).isEmpty());

    Stream.generate(unknownKeySupplier).limit(count)
        .forEach(key -> doDelete(null, false, key, valueSupplier.get()));

    map.forEach((key, value) -> doDelete(null, true, key, value));
    map.keySet().forEach(key -> assertThat(adapter.get(key)).isEmpty());
  }

  public void testPutDeleteGet(int count, PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate) {
    SetMultimap<K, V> map = HashMultimap.create();
    Supplier<K> existingKeySupplier = () -> randomElement(map.keys());

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
          V v = known ? randomElement(map.get(key)) : valueSupplier.get();
          doDelete(map, known, key, v);
        }
      }
      runCallback(iterationCallback, adapter.storage);
    });
  }

  private <X> X randomElement(Collection<X> xs) {
    Preconditions.checkNotNull(xs);
    Preconditions.checkState(xs.size() > 0);
    int index = random.nextInt(xs.size());
    if (xs instanceof List && xs instanceof RandomAccess) {
      return ((List<X>) xs).get(index);
    }
    return xs.stream().skip(index).findAny().get();
  }

  private void doDelete(@Nullable SetMultimap<K, V> map, boolean known, K key, V value) {
    print("del ", key, value);
    adapter.delete(key, value);
    if (known && map != null) {
      map.remove(key, value);
    }
  }

  private void doGet(SetMultimap<K, V> map, boolean known, K key) {
    print("get " + key);
    var a = assertThat(adapter.get(key)).describedAs("key=%s", key);
    if (known) {
      a.containsExactlyInAnyOrderElementsOf(map.get(key));
    } else {
      a.isEmpty();
    }
  }

  private void doPut(SetMultimap<K, V> map, K key) {
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
    private final MultiValueReadWriteStorage<K, V> storage;

    private Adapter(MultiValueReadWriteStorage<K, V> storage) {
      this.storage = Preconditions.checkNotNull(storage);
    }

    private void put(K key, V value) {
      try {
        storage.put(key, value);
      } catch (IOException e) {
        throw new AssertionError(
            Strings.lenientFormat("unexpected IOError for put() %s=%s of %s", key, value, storage), e);
      }
    }

    @Nullable List<V> get(K key) {
      try {
        var result = storage.get(key);
        if (result == null) {
          return List.of();
        }
        var list = new ArrayList<V>(result.count());
        var iterator = result.valueIterator();
        while (iterator.hasNext()) {
          list.add(iterator.next());
        }
        result.close();
        return list;
      } catch (IOException e) {
        throw new AssertionError(
            Strings.lenientFormat("unexpected IOError for get() %s of %s", key, storage), e);
      }
    }

    void delete(K key, V value) {
      try {
        storage.delete(key, value);
      } catch (IOException e) {
        throw new AssertionError(
            Strings.lenientFormat("unexpected IOError for delete() %s of %s", key, storage), e);
      }
    }
  }
}

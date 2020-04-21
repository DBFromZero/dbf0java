package dbf0.disk_key_value.readwrite.btree;

import com.squareup.burst.BurstJUnit4;
import com.squareup.burst.annotation.Burst;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(BurstJUnit4.class)
public class BTreeTest {

  @Burst Capacity capacity;

  private enum Capacity {
    C2(2),
    C4(4),
    C8(8),
    C128(128);

    Capacity(int capacity) {
      this.capacity = capacity;
    }

    private final int capacity;
  }

  private enum Count {
    N2(2),
    N4(4),
    N10(10),
    N50(50),
    N100(100);

    Count(int count) {
      this.count = count;
    }

    private final int count;

    private IntStream range() {
      return IntStream.range(0, count);
    }

    private <T> Stream<T> times(Supplier<T> supplier) {
      return range().mapToObj(ignored -> supplier.get());
    }
  }

  private enum RandomSeed {
    CAFE(0xCAFE),
    DEADBEEF(0xDEADBEEF),
    BAD(0xBAD);

    private final long seed;

    RandomSeed(long seed) {
      this.seed = seed;
    }

    private Random random() {
      return new Random(seed);
    }
  }

  private enum KeySetSize {
    S1(1),
    S10(10),
    S100(100),
    S1000(1000);

    private final int size;

    KeySetSize(int size) {
      this.size = size;
    }
  }

  @Test public void testEmpty() {
    var btree = bTree();
    assertThat(btree.size()).isEqualTo(0);
    assertThat(btree.get(0)).isNull();
  }

  @Test public void testPutSingle() {
    var btree = bTree();
    btree.put(1, 2);
    assertThat(btree.size()).isEqualTo(1);
    assertThat(btree.get(1)).isEqualTo(2);
  }

  @Test public void testDeleteEmpty() {
    var btree = bTree();
    var deleted = btree.delete(1);
    assertThat(deleted).isFalse();
    assertThat(btree.size()).isEqualTo(0);
    assertThat(btree.get(1)).isNull();
  }

  @Test public void testDeleteSingle() {
    var btree = bTree();
    btree.put(1, 2);
    var deleted = btree.delete(1);
    assertThat(deleted).isTrue();
    assertThat(btree.size()).isEqualTo(0);
    assertThat(btree.get(1)).isNull();
  }

  @Test public void testReplaceSingle() {
    var btree = bTree();
    btree.put(1, 2);
    assertThat(btree.size()).isEqualTo(1);
    assertThat(btree.get(1)).isEqualTo(2);
    btree.put(1, 3);
    assertThat(btree.size()).isEqualTo(1);
    assertThat(btree.get(1)).isEqualTo(3);
  }

  public void testRunAddDeleteMan() {
    capacity = Capacity.C4;
    testAddDeleteMany(RandomSeed.CAFE, Count.N100, KeySetSize.S1000);
  }

  @Test public void testAddDeleteMany(RandomSeed seed, Count count, KeySetSize keySetSize) {
    var random = seed.random();
    Map<Integer, Integer> map = new HashMap<>(count.count);
    var btree = bTree();
    count.range().forEach(ignored -> {
      var key = random.nextInt(keySetSize.size);
      var value = random.nextInt();
      map.put(key, value);
      btree.put(key, value);
      assertThat(btree.size()).isEqualTo(map.size());
    });
  /*
    map.forEach((key, value) -> assertThat(btree.get(key)).isEqualTo(value));

    count.times(random::nextInt).filter(Predicate.not(map::containsKey))
        .forEach(key -> assertThat(btree.get(key)).isNull());

    count.times(random::nextInt).filter(Predicate.not(map::containsKey))
        .forEach(key -> assertThat(btree.delete(key)).isFalse());

    map.keySet().forEach(key -> assertThat(btree.delete(key)).isTrue());

    assertThat(btree.size()).isEqualTo(0);
    */
  }

  private BTree<Integer, Integer> bTree() {
    return new BTree<>(capacity.capacity);
  }
}

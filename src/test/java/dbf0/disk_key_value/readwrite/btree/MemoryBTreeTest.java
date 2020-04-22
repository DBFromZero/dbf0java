package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.squareup.burst.BurstJUnit4;
import com.squareup.burst.annotation.Burst;
import dbf0.test.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(BurstJUnit4.class)
public class MemoryBTreeTest {

  private static final boolean DEBUG = false;

  public enum Capacity {
    C2(2),
    C4(4),
    C8(8),
    C128(128);

    Capacity(int capacity) {
      this.capacity = capacity;
    }

    public final int capacity;
  }

  @Burst Capacity capacity;

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

    map.forEach((key, value) -> assertThat(btree.get(key)).describedAs("key=%s", key).isEqualTo(value));

    count.times(random::nextInt).filter(Predicate.not(map::containsKey))
        .forEach(key -> assertThat(btree.get(key)).isNull());

    count.times(random::nextInt).filter(Predicate.not(map::containsKey))
        .forEach(key -> assertThat(btree.delete(key)).isFalse());

    map.keySet().forEach(key -> assertThat(btree.delete(key)).describedAs("key=%s", key).isTrue());

    assertThat(btree.size()).isEqualTo(0);
    var idsList = btree.streamIdsInUse().collect(Collectors.toList());
    assertThat(idsList).hasSize(1);
    assertThat(btree.getStorage().getIdsInUse()).hasSize(1);
    assertThat(idsList).hasSameElementsAs(btree.getStorage().getIdsInUse());
  }

  public void testManualBurst() {
    capacity = Capacity.C2;
    testPutDeleteGet(RandomSeed.CAFE, Count.N50, KeySetSize.S10, PutDeleteGet.DELETE_HEAVY, KnownKeyRate.LOW);
  }

  @Test public void testPutDeleteGet(RandomSeed seed, Count count, KeySetSize keySetSize,
                                     PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate) {
    var random = seed.random();
    Map<Integer, Integer> map = new HashMap<>(count.count);
    var btree = bTree();

    Supplier<Integer> getKnownKey = () ->
        map.keySet().stream().skip(random.nextInt(map.size())).findAny().get();

    count.range().forEach(ignored -> {
      var known = (!map.isEmpty()) && random.nextDouble() < knownKeyRate.rate;
      var r = random.nextDouble();
      if (r < putDeleteGet.putRate) {
        var value = random.nextInt();
        var key = known ? getKnownKey.get() : random.nextInt(keySetSize.size);
        print("put " + key);
        Preconditions.checkState(!known || map.containsKey(key));
        map.put(key, value);
        btree.put(key, value);
      } else {
        var key = known ? getKnownKey.get() : keySetSize.size + random.nextInt(keySetSize.size);
        Preconditions.checkState(known == map.containsKey(key));
        r -= putDeleteGet.putRate;
        if (r < putDeleteGet.getRate) {
          print("get " + key);
          var a = assertThat(btree.get(key)).describedAs("key=%s", key);
          if (known) {
            a.isEqualTo(map.get(key));
          } else {
            a.isNull();
          }
        } else {
          print("del " + key);
          var a = assertThat(btree.delete(key)).describedAs("key=%s", key);
          if (known) {
            map.remove(key);
            a.isTrue();
          } else {
            a.isFalse();
          }
        }
      }
      assertThat(btree.size()).isEqualTo(map.size());

      var idsList = btree.streamIdsInUse().collect(Collectors.toList());
      assertThat(idsList).doesNotHaveDuplicates();
      assertThat(idsList).hasSameElementsAs(btree.getStorage().getIdsInUse());
    });
  }

  private MemoryBTree<Integer, Integer> bTree() {
    return new MemoryBTree<>(capacity.capacity, new MemoryBTeeStorage<Integer, Integer>());
  }

  private void print(Object... args) {
    if (DEBUG) {
      System.out.println(Joiner.on(" ").join(args));
    }
  }
}

package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Joiner;
import com.squareup.burst.BurstJUnit4;
import com.squareup.burst.annotation.Burst;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.test.*;
import org.junit.Test;
import org.junit.runner.RunWith;

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
    var btree = bTree();

    ReadWriteStorageTester.builderForInteger(btree, seed, keySetSize)
        .setDebug(DEBUG)
        .build()
        .testAddDeleteMany(count.count);

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
    var btree = bTree();
    ReadWriteStorageTester.builderForInteger(btree, seed, keySetSize)
        .setDebug(DEBUG)
        .setIterationCallback((ignored) -> {
          var idsList = btree.streamIdsInUse().collect(Collectors.toList());
          assertThat(idsList).doesNotHaveDuplicates();
          assertThat(idsList).hasSameElementsAs(btree.getStorage().getIdsInUse());
        })
        .build()
        .testPutDeleteGet(count.count, putDeleteGet, knownKeyRate);
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

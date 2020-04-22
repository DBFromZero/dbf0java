package dbf0.disk_key_value.readwrite.btree;

import com.squareup.burst.BurstJUnit4;
import com.squareup.burst.annotation.Burst;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.test.*;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BurstJUnit4.class)
public class MemoryBTreeTest extends BaseBTreeTest {

  private static final boolean DEBUG = false;

  @Burst Capacity capacity;

  public void testManualBurst() {
    capacity = Capacity.C2;
    testPutDeleteGet(RandomSeed.CAFE, Count.N50, KeySetSize.S10, PutDeleteGet.DELETE_HEAVY, KnownKeyRate.LOW);
  }

  @Test public void testPutDeleteGet(RandomSeed seed, Count count, KeySetSize keySetSize,
                                     PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate) {
    var btree = bTree();
    ReadWriteStorageTester.builderForIntegers(btree, seed, keySetSize)
        .setDebug(DEBUG)
        .setIterationCallback((ignored) -> validateIdsInUse(btree))
        .build()
        .testPutDeleteGet(count.count, putDeleteGet, knownKeyRate);
  }

  @Override protected MemoryBTree<Integer, Integer> bTree() {
    return new MemoryBTree<>(capacity.capacity, new MemoryBTeeStorage<Integer, Integer>());
  }
}

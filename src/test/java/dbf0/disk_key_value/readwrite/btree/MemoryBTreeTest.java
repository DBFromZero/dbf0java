package dbf0.disk_key_value.readwrite.btree;

import com.squareup.burst.BurstJUnit4;
import com.squareup.burst.annotation.Burst;
import dbf0.test.*;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(BurstJUnit4.class)
public class MemoryBTreeTest extends BaseBTreeTest {

  private static final boolean DEBUG = false;

  @Burst Capacity capacity;

  public void testManualBurst() throws IOException {
    capacity = Capacity.C2;
    testPutDeleteGet(RandomSeed.CAFE, Count.N50, KeySetSize.S10, PutDeleteGet.DELETE_HEAVY, KnownKeyRate.LOW);
  }

  @Override protected boolean isDebug() {
    return DEBUG;
  }

  @Override protected MemoryBTree<Integer, Integer> bTree() {
    return new MemoryBTree<>(new BTreeConfig(capacity.capacity));
  }
}

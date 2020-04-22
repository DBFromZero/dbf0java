package dbf0.disk_key_value.readwrite.btree;

import com.squareup.burst.annotation.Burst;
import dbf0.disk_key_value.readwrite.blocks.MemoryBlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MemoryMetadataStorage;
import dbf0.disk_key_value.readwrite.blocks.SerializationPair;
import dbf0.test.Count;
import dbf0.test.KeySetSize;
import dbf0.test.RandomSeed;
import org.junit.Test;

import java.io.IOException;

//@RunWith(BurstJUnit4.class)
public class BlockBTreeTest extends BaseBTreeTest {

  private static final boolean DEBUG = true;

  @Burst Capacity capacity;

  @Test public void testManualBurst() throws IOException {
    capacity = Capacity.C2;
    testAddDeleteMany(RandomSeed.CAFE, Count.N4, KeySetSize.S10);
  }

  @Override protected boolean isDebug() {
    return DEBUG;
  }

  @Override protected BlockBTree<Integer, Integer> bTree() throws IOException {
    var btree = new BlockBTree<>(capacity.capacity, new BlockBTreeStorage<>(
        new MemoryMetadataStorage(),
        new MemoryBlockStorage(),
        new NodeSerialization<>(
            capacity.capacity,
            SerializationPair.intSerializationPair(),
            SerializationPair.intSerializationPair())));
    btree.initialize();
    return btree;
  }
}

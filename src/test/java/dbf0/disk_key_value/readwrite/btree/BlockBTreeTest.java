package dbf0.disk_key_value.readwrite.btree;

import com.squareup.burst.BurstJUnit4;
import com.squareup.burst.annotation.Burst;
import dbf0.disk_key_value.readwrite.blocks.MemoryBlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MemoryMetadataMap;
import dbf0.disk_key_value.readwrite.blocks.SerializationPair;
import dbf0.test.Count;
import dbf0.test.KeySetSize;
import dbf0.test.RandomSeed;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(BurstJUnit4.class)
public class BlockBTreeTest extends BaseBTreeTest {

  private static final boolean DEBUG = false;

  @Burst Capacity capacity;

  //@Test
  public void testManualBurst() throws IOException {
    capacity = Capacity.C2;
    testAddDeleteMany(RandomSeed.CAFE, Count.N10, KeySetSize.S1000);
  }

  @Override protected boolean isDebug() {
    return DEBUG;
  }

  @Override protected BlockBTree<Integer, Integer> bTree() throws IOException {
    var config = new BTreeConfig(capacity.capacity);
    var btree = new BlockBTree<>(new BlockBTreeStorage<>(
        config,
        new MemoryMetadataMap<>(),
        new MemoryBlockStorage(),
        new NodeSerialization<>(
            config,
            SerializationPair.intSerializationPair(),
            SerializationPair.intSerializationPair())));
    btree.initialize();
    return btree;
  }
}

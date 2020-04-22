package dbf0.disk_key_value.readwrite.btree;

import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.disk_key_value.readwrite.blocks.FileBlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MemoryMetadataStorage;
import dbf0.disk_key_value.readwrite.blocks.SerializationPair;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TmpFileBlockStorageTest {

  @Test public void testIt() throws IOException {
    var file = new File("/data/tmp/btree_test");
    if (file.exists()) {
      var deleted = file.delete();
      assertThat(deleted).isTrue();
    }
    var config = new BTreeConfig(8);
    var blockStorage = new FileBlockStorage(file, new MemoryMetadataStorage());
    var btree = new BlockBTree<>(new BlockBTreeStorage<>(
        config,
        new MemoryMetadataStorage(),
        blockStorage,
        new NodeSerialization<>(
            config,
            SerializationPair.bytesSerializationPair(),
            SerializationPair.bytesSerializationPair())));
    blockStorage.initialize();
    btree.initialize();

    ReadWriteStorageTester.builderForBytes(btree, RandomSeed.CAFE.random(), 16, 2048)
        .setDebug(true)
        .setIterationCallback((ignored) -> System.out.println(blockStorage.getStats()))
        .build()
        .testPutDeleteGet(100 * 1000, PutDeleteGet.PUT_HEAVY, KnownKeyRate.HIGH);
  }
}

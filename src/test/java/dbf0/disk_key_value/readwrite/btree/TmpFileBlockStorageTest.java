package dbf0.disk_key_value.readwrite.btree;

import dbf0.common.Dbf0Util;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

public class TmpFileBlockStorageTest {

  @Test public void testIt() throws IOException {
    Dbf0Util.enableConsoleLogging(Level.FINEST);
    var file = new File("/data/tmp/btree_test");
    if (file.exists()) {
      var deleted = file.delete();
      assertThat(deleted).isTrue();
    }
    var config = new BTreeConfig(8, 16);
    var blockStorage = FileBlockStorage.forFile(file, new MemoryMetadataStorage());
    var bTreeStorage = new BlockBTreeStorage<>(
        config,
        new MemoryMetadataStorage(),
        blockStorage,
        new NodeSerialization<>(
            config,
            SerializationPair.bytesSerializationPair(),
            SerializationPair.bytesSerializationPair()));
    var btree = new BlockBTree<>(bTreeStorage);
    blockStorage.initialize();
    btree.initialize();

    var counter = new AtomicInteger(0);
    ReadWriteStorageTester.builderForBytes(btree, RandomSeed.CAFE.random(), 16, 4096)
        .setDebug(true)
        .setIterationCallback((ignored) -> {
          var count = counter.incrementAndGet();
          var stats = blockStorage.getStats();
          System.out.println(count + " " + stats);
          if (stats.unusedBytesFraction() > 0.8) {
            var vacuum = bTreeStorage.vacuum();
            try {
              vacuum.writeNewFile();
              vacuum.commit();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        })
        .build()
        .testPutDeleteGet(100 * 1000, PutDeleteGet.PUT_HEAVY, KnownKeyRate.LOW);
  }
}

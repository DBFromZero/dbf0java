package dbf0.disk_key_value.readwrite.btree;

import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.SerializationPair;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.disk_key_value.readwrite.blocks.FileBlockStorage;
import dbf0.disk_key_value.readwrite.blocks.MemoryMetadataMap;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlockBTreeStorageWithVacuumingTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(BlockBTreeStorageWithVacuumingTest.class);

  @Test public void testIt() throws IOException {
    Dbf0Util.enableConsoleLogging(Level.INFO, true);

    var config = new BTreeConfig(8, 16);

    var blockStorage = FileBlockStorage.inMemory();
    var bTreeStorage = new BlockBTreeStorage<>(
        config,
        new MemoryMetadataMap<>(),
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
        .debug(false)
        .iterationCallback((ignored) -> {
          var count = counter.incrementAndGet();
          if (count % 100 == 0) {
            var stats = blockStorage.getStats();
            LOGGER.info("iteration " + count + " " + stats);
            if (stats.totalBytes() > 1e6 && stats.unusedBytesFraction() > 0.8) {
              var vacuum = bTreeStorage.vacuum();
              try {
                vacuum.writeNewFile();
                vacuum.commit();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }
        })
        .build()
        .testPutDeleteGet(10 * 1000, PutDeleteGet.BALANCED, KnownKeyRate.MID);
  }
}

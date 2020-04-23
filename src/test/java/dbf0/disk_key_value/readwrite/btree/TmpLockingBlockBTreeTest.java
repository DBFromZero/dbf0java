package dbf0.disk_key_value.readwrite.btree;

import com.google.common.collect.Streams;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.disk_key_value.readwrite.blocks.*;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static dbf0.disk_key_value.readwrite.btree.TmpFileBlockStorageTest.deleteFile;

public class TmpLockingBlockBTreeTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(TmpLockingBlockBTreeTest.class);

  @Test public void testIt() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINEST);


    var file = new File("/data/tmp/btree_test");
    var metadataFile = new File("/data/tmp/btree_test_metadata");

    deleteFile(file);
    deleteFile(metadataFile);
    var config = new BTreeConfig(8, 16);
    var metadataStore = new FileMetadataStorage<>(new FileOperationsImpl(metadataFile, "-tmp"));
    metadataStore.initialize();

    var blockStorage = FileBlockStorage.forFile(file, metadataStore);
    var bTreeStorage = new BlockBTreeStorage<>(
        config,
        metadataStore.newMap("btree", SerializationHelper::writeLong, SerializationHelper::writeLong),
        blockStorage,
        new NodeSerialization<>(
            config,
            SerializationPair.bytesSerializationPair(),
            SerializationPair.bytesSerializationPair()));
    var baseBTree = new BlockBTree<>(bTreeStorage);
    blockStorage.initialize();

    var btree = new LockingBlockBTree<>(baseBTree, bTreeStorage, () -> {
      var stats = blockStorage.getStats();
      return stats.unusedBytesFraction() > 0.8 & stats.getUnused().getBytes() > 1e7;
    });

    blockStorage.initialize();
    btree.initialize();

    var threads = Streams.concat(
        Stream.of(createThread(btree, 8, PutDeleteGet.PUT_HEAVY, KnownKeyRate.LOW, true,
            blockStorage)),
        IntStream.range(0, 4).mapToObj(i ->
            createThread(btree, 10 + i, PutDeleteGet.GET_HEAVY, KnownKeyRate.HIGH, false,
                blockStorage)
        )).collect(Collectors.toList());

    threads.forEach(Thread::start);

    for (var thread : threads) {
      thread.join();
    }
  }

  private Thread createThread(LockingBlockBTree<ByteArrayWrapper, ByteArrayWrapper> btree,
                              int keySize, PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate,
                              boolean callback, BlockStorage blockStorage) {
    var builder = ReadWriteStorageTester.builderForBytes(btree, RandomSeed.CAFE.random(), keySize, 4096)
        .setDebug(false)
        .setCheckSize(false);
    if (callback) {
      var count = new AtomicInteger(0);
      builder.setIterationCallback((ignored) -> {
        if (count.incrementAndGet() % 50 == 0) {
          var size = btree.size();
          var stats = btree.withReadLock(blockStorage::getStats);
          LOGGER.info("size: " + size + " stats: " + stats);
        }
      });
    }
    var tester = builder.build();
    return new Thread(() -> {
      try {
        tester.testPutDeleteGet(100 * 1000, putDeleteGet, knownKeyRate);
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, e, () -> "error in thread");
      }
    });
  }
}
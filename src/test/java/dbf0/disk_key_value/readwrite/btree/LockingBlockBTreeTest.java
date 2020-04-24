package dbf0.disk_key_value.readwrite.btree;

import com.google.common.collect.Streams;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadTwoStepWriteLock;
import dbf0.disk_key_value.io.SerializationPair;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.disk_key_value.readwrite.blocks.BlockStorage;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;


public class LockingBlockBTreeTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(LockingBlockBTreeTest.class);

  @Test public void testIt() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.INFO, true);
    Dbf0Util.getLogger(ParentNode.class).setLevel(Level.INFO);
    Dbf0Util.getLogger(Node.class).setLevel(Level.INFO);
    Dbf0Util.getLogger(LeafNode.class).setLevel(Level.INFO);


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
    var baseBTree = new BlockBTree<>(bTreeStorage);
    blockStorage.initialize();

    var lock = new ReadTwoStepWriteLock();
    var btree = new LockingBlockBTree<>(baseBTree, bTreeStorage, () -> {
      try {
        var stats = lock.callWithReadLock(blockStorage::getStats);
        return stats.unusedBytesFraction() > 0.8 & stats.getUnused().getBytes() > 1e7;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, lock);

    blockStorage.initialize();
    btree.initialize();

    var errors = new AtomicInteger(0);
    var threads = Streams.concat(
        Stream.of(createThread(btree, 8, PutDeleteGet.PUT_HEAVY, KnownKeyRate.LOW, true,
            blockStorage, lock, errors)),
        IntStream.range(0, 4).mapToObj(i ->
            createThread(btree, 10 + i, PutDeleteGet.GET_HEAVY, KnownKeyRate.HIGH, false,
                blockStorage, lock, errors)
        )).collect(Collectors.toList());

    threads.forEach(Thread::start);

    for (var thread : threads) {
      thread.join();
    }
  }

  private Thread createThread(LockingBlockBTree<ByteArrayWrapper, ByteArrayWrapper> btree,
                              int keySize, PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate,
                              boolean callback, BlockStorage blockStorage, ReadTwoStepWriteLock lock,
                              AtomicInteger errors) {
    var builder = ReadWriteStorageTester.builderForBytes(btree, RandomSeed.CAFE.random(), keySize, 4096)
        .debug(false)
        .checkSize(false);
    if (callback) {
      var count = new AtomicInteger(0);
      builder.iterationCallback((ignored) -> {
        assertThat(errors.get()).isZero();
        if (count.incrementAndGet() % 50 == 0) {
          var size = btree.size();
          var stats = lock.callWithReadLock(blockStorage::getStats);
          LOGGER.info(count.get() + " size=" + size + " stats: " + stats);
        }
      });
    } else {
      builder.iterationCallback((ignored) -> assertThat(errors.get()).isZero());
    }
    var tester = builder.build();
    return new Thread(() -> {
      try {
        tester.testPutDeleteGet(10 * 1000, putDeleteGet, knownKeyRate);
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, e, () -> "error in thread");
        errors.incrementAndGet();
      }
    });
  }
}

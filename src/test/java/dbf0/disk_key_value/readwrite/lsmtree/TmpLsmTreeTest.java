package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.collect.Streams;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileOperationsImpl;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static dbf0.disk_key_value.readwrite.btree.TmpFileBlockStorageTest.deleteFile;

public class TmpLsmTreeTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(TmpLsmTreeTest.class);

  @Test public void testIt() throws Exception {
    //Dbf0Util.getLogger(ReadWriteStorageTester.class).setLevel(Level.FINEST);
    Dbf0Util.enableConsoleLogging(Level.INFO);

    var baseFile = new File("/data/tmp/lsm_base");
    var indexFile = new File("/data/tmp/lsm_index");

    deleteFile(baseFile);
    deleteFile(indexFile);

    var tree = new LsmTree<>(
        10 * 1000,
        10,
        new FileOperationsImpl(baseFile, "-tmp"),
        new FileOperationsImpl(indexFile, "-tmp")
    );

    if (false) {
      var builder = ReadWriteStorageTester.builderForBytes(tree, RandomSeed.CAFE.random(), 16, 4096)
          .setDebug(true)
          .setCheckSize(true);
      var count = new AtomicInteger(0);
      builder.setIterationCallback((ignored) -> {
        if (count.incrementAndGet() % 1000 == 0) {
          var size = tree.size();
          LOGGER.info("size: " + tree.size());
        }
      });
      var tester = builder.build();
      tester.testPutDeleteGet(100 * 1000, PutDeleteGet.PUT_HEAVY, KnownKeyRate.MID);
    }

    if (true) {
      var threads = Streams.concat(
          Stream.of(createThread(tree, 16, PutDeleteGet.PUT_HEAVY, KnownKeyRate.LOW, true)),
          IntStream.range(0, 8).mapToObj(i ->
              createThread(tree, 16, PutDeleteGet.GET_HEAVY, KnownKeyRate.HIGH, false)
          )).collect(Collectors.toList());
      threads.forEach(Thread::start);
      for (var thread : threads) {
        thread.join();
      }
    }
  }

  private Thread createThread(LsmTree<?> tree,
                              int keySize, PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate,
                              boolean callback) {
    var builder = ReadWriteStorageTester.builderForBytes(tree, new Random(), keySize, 4096)
        .setDebug(false)
        .setCheckSize(false);
    if (callback) {
      var count = new AtomicInteger(0);
      builder.setIterationCallback((ignored) -> {
        if (count.incrementAndGet() % 1000 == 0) {
          var size = tree.size();
          LOGGER.info("size: " + tree.size());
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

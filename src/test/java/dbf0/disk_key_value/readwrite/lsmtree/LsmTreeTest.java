package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.collect.Streams;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class LsmTreeTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTreeTest.class);

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.INFO, true);
  }

  @Test public void testSingleThreaded() {
    var create = createTree(1000);
    var operations = create.getLeft();
    var tree = create.getRight();
    var builder = ReadWriteStorageTester.builderForBytes(tree, RandomSeed.CAFE.random(), 16, 4096)
        .debug(false)
        .checkDeleteReturnValue(false)
        .checkSize(false);
    var count = new AtomicInteger(0);
    builder.iterationCallback((ignored) -> {
      if (count.incrementAndGet() % 1000 == 0) {
        LOGGER.info("iteration " + count.get() + " size " + Dbf0Util.formatBytes(operations.size()));
      }
    });
    var tester = builder.build();
    tester.testPutDeleteGet(10 * 1000, PutDeleteGet.PUT_HEAVY, KnownKeyRate.MID);
  }

  @NotNull
  public Pair<MemoryFileOperations, LsmTree<MemoryFileOperations.MemoryOutputStream>> createTree(int mergedThreshold) {
    var baseFileOperations = new MemoryFileOperations();
    return Pair.of(baseFileOperations, new LsmTree<>(
        mergedThreshold,
        10,
        baseFileOperations,
        new MemoryFileOperations()
    ));
  }

  @Test public void testMultiThread() throws Exception {
    var create = createTree(10 * 1000);
    var operations = create.getLeft();
    var tree = create.getRight();
    var errors = new AtomicInteger(0);
    var threads = Streams.concat(
        Stream.of(
            createThread(tree, PutDeleteGet.PUT_HEAVY, KnownKeyRate.LOW, true, errors, operations),
            createThread(tree, PutDeleteGet.DELETE_HEAVY, KnownKeyRate.HIGH, false, errors, operations)),
        IntStream.range(0, 8).mapToObj(i ->
            createThread(tree, PutDeleteGet.GET_HEAVY, KnownKeyRate.HIGH, false, errors, operations)
        )).collect(Collectors.toList());
    threads.forEach(Thread::start);
    for (var thread : threads) {
      thread.join();
    }
    assertThat(errors.get()).isZero();
  }

  private Thread createThread(LsmTree<?> tree,
                              PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate,
                              boolean callback, AtomicInteger errors, MemoryFileOperations operations) {
    var builder = ReadWriteStorageTester.builderForBytes(tree, new Random(), 16, 4096)
        .debug(false)
        .checkSize(false)
        .checkDeleteReturnValue(false);
    if (callback) {
      var count = new AtomicInteger(0);
      builder.iterationCallback((ignored) -> {
        assertThat(errors.get()).isZero();
        if (count.incrementAndGet() % 1000 == 0) {
          LOGGER.info("iteration " + count.get() + " size " + Dbf0Util.formatBytes(operations.size()));
        }
      });
    } else {
      builder.iterationCallback((ignored) -> assertThat(errors.get()).isZero());
    }
    var tester = builder.build();
    return new Thread(() -> {
      try {
        tester.testPutDeleteGet(50 * 1000, putDeleteGet, knownKeyRate);
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, e, () -> "error in thread");
        errors.incrementAndGet();
      }
    });
  }
}

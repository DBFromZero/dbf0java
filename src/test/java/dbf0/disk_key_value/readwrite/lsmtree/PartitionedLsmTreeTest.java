package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.collect.Streams;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.ReadOnlyFileOperations;
import dbf0.disk_key_value.readwrite.HashPartitionedReadWriteStorage;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionedLsmTreeTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(PartitionedLsmTreeTest.class);

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testSingleThreaded() throws IOException {
    var directoryAndStorage = createPartitionedLsmTree(1000);
    var operations = directoryAndStorage.getLeft();
    var tree = directoryAndStorage.getRight();
    var builder = ReadWriteStorageTester.builderForBytes(tree, RandomSeed.CAFE.random(), 16, 4096)
        .debug(true)
        .checkDeleteReturnValue(false)
        .checkSize(false);
    var count = new AtomicInteger(0);
    builder.iterationCallback((ignored) -> {
      if (count.incrementAndGet() % 1000 == 0) {
        LOGGER.info("iteration " + count.get() + " size " + Dbf0Util.formatBytes(getDirectorySize(operations)));
      }
    });
    var tester = builder.build();
    tester.testPutDeleteGet(10 * 1000, PutDeleteGet.BALANCED, KnownKeyRate.MID);
  }

  @Test public void testMultiThread() throws Exception {
    var directoryAndStorage = createPartitionedLsmTree(1000);
    var operations = directoryAndStorage.getLeft();
    var tree = directoryAndStorage.getRight();
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

  private Thread createThread(HashPartitionedReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> store,
                              PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate,
                              boolean callback, AtomicInteger errors, MemoryFileDirectoryOperations operations) {
    var builder = ReadWriteStorageTester.builderForBytes(store, new Random(), 16, 4096)
        .debug(false)
        .checkSize(false)
        .checkDeleteReturnValue(false);
    if (callback) {
      var count = new AtomicInteger(0);
      builder.iterationCallback((ignored) -> {
        assertThat(errors.get()).isZero();
        if (count.incrementAndGet() % 1000 == 0) {
          LOGGER.info("iteration " + count.get() + " size " + Dbf0Util.formatBytes(getDirectorySize(operations)));
        }
      });
    } else {
      builder.iterationCallback((ignored) -> assertThat(errors.get()).isZero());
    }
    var tester = builder.build();
    return new Thread(() -> {
      try {
        tester.testPutDeleteGet(25 * 1000, putDeleteGet, knownKeyRate);
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, e, () -> "error in thread");
        errors.incrementAndGet();
      }
    });
  }

  private Pair<MemoryFileDirectoryOperations, HashPartitionedReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper>>
  createPartitionedLsmTree(int pendingWritesDeltaThreshold) throws IOException {
    var directoryOperations = new MemoryFileDirectoryOperations();
    var executor = Executors.newScheduledThreadPool(4);
    var store = HashPartitionedReadWriteStorage.create(4,
        partition -> {
          var tree = LsmTree.builderForTesting(directoryOperations.subDirectory(String.valueOf(partition)))
              .withPendingWritesDeltaThreshold(pendingWritesDeltaThreshold)
              .withScheduledExecutorService(executor)
              .withIndexRate(10)
              .withMaxInFlightWriteJobs(10)
              .withMaxDeltaReadPercentage(0.5)
              .withMergeCronFrequency(Duration.ofMillis(100))
              .build();
          tree.initialize();
          return tree;
        });
    return Pair.of(directoryOperations, store);
  }

  private long getDirectorySize(MemoryFileDirectoryOperations d) throws IOException {
    return d.list().stream().map(d::subDirectory).flatMap(dd -> dd.list().stream())
        .map(d::file).mapToLong(ReadOnlyFileOperations::length).sum();
  }
}

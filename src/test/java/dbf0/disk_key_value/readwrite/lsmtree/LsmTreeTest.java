package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.collect.Streams;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FlakyLengthMemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.io.ReadOnlyFileOperations;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.disk_key_value.readwrite.ReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.log.ImmediateLogSynchronizer;
import dbf0.disk_key_value.readwrite.log.WriteAheadLog;
import dbf0.document.types.DString;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import io.vavr.control.Either;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LsmTreeTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTreeTest.class);

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testSingleThreaded() throws IOException {
    var lsmTree = createLsmTree(1000);
    var operations = lsmTree.getLeft();
    var tree = lsmTree.getRight();
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
    var create = createLsmTree(5 * 1000);
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

  @Test public void testLoadExisting() throws IOException {
    var directoryOperations = new MemoryFileDirectoryOperations();
    var createLsmTree = (Supplier<ReadWriteStorageWithBackgroundTasks<ByteArrayWrapper, ByteArrayWrapper>>) () ->
        LsmTree.builderForTesting(directoryOperations)
            .withPendingWritesDeltaThreshold(1000)
            .withScheduledThreadPool(2)
            .withIndexRate(10)
            .withMaxInFlightWriteJobs(10)
            .withMaxDeltaReadPercentage(0.5)
            .withMergeCronFrequency(Duration.ofMillis(100))
            .buildWithBackgroundTasks();

    var initialTree = createLsmTree.get();
    initialTree.initialize();

    var map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    var random = RandomSeed.CAFE.random();
    for (int i = 0; i < 1500; i++) {
      var key = ByteArrayWrapper.random(random, 16);
      var value = ByteArrayWrapper.random(random, 100);
      map.put(key, value);
      initialTree.put(key, value);
    }
    initialTree.close();

    var readingTree = createLsmTree.get();
    readingTree.initialize();

    for (var entry : map.entrySet()) {
      assertThat(readingTree.get(entry.getKey())).isEqualTo(entry.getValue());
    }
    readingTree.close();
  }

  @Test
  public void testRecoverFromCrashUsingLogs() throws IOException {
    var logDirectory = new MemoryFileDirectoryOperations();
    var createLsmTree = (Function<MemoryFileDirectoryOperations, ReadWriteStorageWithBackgroundTasks<ByteArrayWrapper, ByteArrayWrapper>>)
        (dataDirectory) ->
            LsmTree.builderForTesting(dataDirectory)
                .withPendingWritesDeltaThreshold(1000)
                .withWriteAheadLog(new WriteAheadLog<>(logDirectory, ImmediateLogSynchronizer.factory()))
                .withScheduledThreadPool(2)
                .withIndexRate(10)
                .withMaxInFlightWriteJobs(10)
                .withMaxDeltaReadPercentage(0.5)
                .withMergeCronFrequency(Duration.ofMillis(250))
                .buildWithBackgroundTasks();

    var flakingTree = createLsmTree.apply(new MemoryFileDirectoryOperations("flaking",
        Map.of("base", Either.left(new FlakyLengthMemoryFileDirectoryOperations(1000)))));
    flakingTree.initialize();

    var map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    var random = RandomSeed.CAFE.random();
    assertThatThrownBy(() -> {
      for (int i = 0; i < 100 * 1000; i++) {
        var key = ByteArrayWrapper.random(random, 16);
        var value = ByteArrayWrapper.random(random, 100);
        flakingTree.put(key, value);
        map.put(key, value);
      }
    }).isInstanceOf(IllegalStateException.class);
    flakingTree.close();

    LOGGER.info("Wrote " + map.size() + " entries before failing");
    assertThat(map).isNotEmpty();

    var readingTree = createLsmTree.apply(new MemoryFileDirectoryOperations());
    readingTree.initialize();

    for (var entry : map.entrySet()) {
      assertThat(readingTree.get(entry.getKey())).isEqualTo(entry.getValue());
    }
    readingTree.close();
  }

  @Test public void testDocumentStoreSingleThreaded() throws IOException {
    var operations = new MemoryFileDirectoryOperations();
    var tree = LsmTree.<MemoryFileOperations.MemoryOutputStream>builderForDocuments()
        .withBaseDeltaFiles(operations)
        .withPendingWritesDeltaThreshold(100)
        .withScheduledThreadPool(2)
        .withIndexRate(10)
        .withMaxInFlightWriteJobs(10)
        .withMaxDeltaReadPercentage(0.5)
        .withMergeCronFrequency(Duration.ofMillis(100))
        .build();
    tree.initialize();
    var random = RandomSeed.CAFE.random();
    var count = new AtomicInteger(0);
    var tester = ReadWriteStorageTester
        .builder(tree)
        .knownKeySupplier(() -> randomDString(random, 4))
        .unknownKeySupplier(() -> randomDString(random, 5))
        .valueSupplier(() -> randomDString(random, random.nextInt(2000)))
        .random(random)
        .debug(false)
        .checkDeleteReturnValue(false)
        .checkSize(false).iterationCallback((ignored) -> {
          if (count.incrementAndGet() % 1000 == 0) {
            LOGGER.info("iteration " + count.get() + " size " + Dbf0Util.formatBytes(getDirectorySize(operations)));
          }
        }).build();
    tester.testPutDeleteGet(20 * 1000, PutDeleteGet.BALANCED, KnownKeyRate.MID);
  }

  private DString randomDString(Random random, int bytes) {
    return DString.of(Hex.encodeHexString(ByteArrayWrapper.random(random, bytes).getArray()));
  }

  private Pair<MemoryFileDirectoryOperations, LsmTree<MemoryFileOperations.MemoryOutputStream, ByteArrayWrapper, ByteArrayWrapper>>
  createLsmTree(int pendingWritesDeltaThreshold) throws IOException {
    var directoryOperations = new MemoryFileDirectoryOperations();
    var tree = LsmTree.builderForTesting(directoryOperations)
        .withPendingWritesDeltaThreshold(pendingWritesDeltaThreshold)
        .withScheduledThreadPool(2)
        .withIndexRate(10)
        .withMaxInFlightWriteJobs(10)
        .withMaxDeltaReadPercentage(0.5)
        .withMergeCronFrequency(Duration.ofMillis(100))
        .build();
    tree.initialize();
    return Pair.of(directoryOperations, tree);
  }


  private long getDirectorySize(MemoryFileDirectoryOperations d) throws IOException {
    return d.list().stream().map(d::file).mapToLong(ReadOnlyFileOperations::length).sum();
  }

  private Thread createThread(LsmTree<?, ByteArrayWrapper, ByteArrayWrapper> tree,
                              PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate,
                              boolean callback, AtomicInteger errors, MemoryFileDirectoryOperations operations) {
    var builder = ReadWriteStorageTester.builderForBytes(tree, new Random(), 16, 4096)
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
}

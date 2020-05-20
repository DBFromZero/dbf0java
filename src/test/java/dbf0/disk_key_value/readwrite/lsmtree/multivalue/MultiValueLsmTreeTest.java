package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.collect.Streams;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.io.IOFunction;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import dbf0.disk_key_value.io.ReadOnlyFileOperations;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorageTester;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.test.KnownKeyRate;
import dbf0.test.PutDeleteGet;
import dbf0.test.RandomSeed;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiValueLsmTreeTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueLsmTreeTest.class);
  public static final LsmTreeConfiguration<ByteArrayWrapper, ValueWrapper<ByteArrayWrapper>> CONFIGURATION =
      LsmTreeConfiguration.builderForMultiValueBytes()
          .withPendingWritesDeltaThreshold(1000)
          .withIndexRate(10)
          .withMaxInFlightWriteJobs(3)
          .withMaxDeltaReadPercentage(0.5)
          .withMergeCronFrequency(Duration.ofMillis(100))
          .build();

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testDoNothing() throws IOException {
    var lsmTree = createLsmTree(1000);
    var tree = lsmTree.getRight();
    tree.close();
  }

  @Test public void testPutGetDelete() throws IOException {
    var lsmTree = createLsmTree(1000);
    var tree = lsmTree.getRight();
    var key = ByteArrayWrapper.of(1);
    var value = ByteArrayWrapper.of(2);

    tree.put(key, value);

    var res = tree.get(key);
    assertThat(res).isNotNull();
    var iterator = res.iterator();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(value);
    assertThat(iterator.hasNext()).isFalse();


    tree.delete(key, value);
    res = tree.get(key);
    assertThat(res.knownSize()).isEqualTo(0);

    tree.close();
  }

  @Test public void testSingleThreaded() throws IOException {
    var lsmTree = createLsmTree(CONFIGURATION.toBuilder()
        .withPendingWritesDeltaThreshold(250)
        .withMergeCronFrequency(Duration.ofHours(10))
        .build());
    var operations = lsmTree.getLeft();
    var tree = lsmTree.getRight();
    var count = new AtomicInteger(0);

    var tester = MultiValueReadWriteStorageTester.builderForBytes(tree, RandomSeed.CAFE.random(), 4, 6)
        .debug(true)
        .iterationCallback((ignored) -> {
          if (count.incrementAndGet() % 1000 == 0) {
            LOGGER.info("iteration " + count.get() + " size " + Dbf0Util.formatBytes(getDirectorySize(operations)));
          }
        }).build();
    tester.testPutDeleteGet(10 * 1000, PutDeleteGet.BALANCED, KnownKeyRate.MID);
    tree.close();
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
    var createLsmTree = (Supplier<MultiValueReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper>>) () ->
        MultiValueLsmTree.builderForBytes(
            CONFIGURATION.toBuilder().withPendingWritesDeltaThreshold(1000000).build(),
            directoryOperations)
            .withScheduledThreadPool(2)
            .buildWithBackgroundTasks();

    var initialTree = createLsmTree.get();
    initialTree.initialize();

    var map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    var random = RandomSeed.CAFE.random();
    for (int i = 0; i < 1500; i++) {
      var key = ByteArrayWrapper.random(random, 8);
      var value = ByteArrayWrapper.random(random, 10);
      map.put(key, value);
      initialTree.put(key, value);
    }
    initialTree.close();

    var readingTree = createLsmTree.get();
    readingTree.initialize();

    for (var entry : map.entrySet()) {
      assertThat(readingTree.get(entry.getKey()))
          .describedAs("key=" + entry.getKey())
          .isNotNull()
          .extracting(IOFunction.wrap(MultiValueReadWriteStorage.Result::realizeRemainingValues))
          .asList()
          .hasSize(1)
          .first()
          .isEqualTo(entry.getValue());
    }
    readingTree.close();
  }

  private Pair<MemoryFileDirectoryOperations, MultiValueReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper>>
  createLsmTree(int pendingWritesDeltaThreshold) throws IOException {
    var configuration = CONFIGURATION.toBuilder().withPendingWritesDeltaThreshold(pendingWritesDeltaThreshold).build();
    return createLsmTree(configuration);
  }

  private Pair<MemoryFileDirectoryOperations, MultiValueReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper>>
  createLsmTree(MemoryFileDirectoryOperations directoryOperations,
                LsmTreeConfiguration<ByteArrayWrapper, ValueWrapper<ByteArrayWrapper>> configuration) throws IOException {
    var tree = MultiValueLsmTree
        .builderForBytes(configuration, directoryOperations)
        .withScheduledThreadPool(2)
        .buildWithBackgroundTasks();
    tree.initialize();
    return Pair.of(directoryOperations, tree);
  }

  @NotNull
  private Pair<MemoryFileDirectoryOperations, MultiValueReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper>>
  createLsmTree(LsmTreeConfiguration<ByteArrayWrapper, ValueWrapper<ByteArrayWrapper>> configuration) throws IOException {
    return createLsmTree(new MemoryFileDirectoryOperations(), configuration);
  }


  private long getDirectorySize(MemoryFileDirectoryOperations d) throws IOException {
    return d.list().stream().map(d::file).mapToLong(ReadOnlyFileOperations::length).sum();
  }

  private Thread createThread(MultiValueReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> tree,
                              PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate,
                              boolean callback, AtomicInteger errors, MemoryFileDirectoryOperations operations) {
    var builder = MultiValueReadWriteStorageTester.builderForBytes(tree, new Random(), 16, 4096)
        .debug(false);
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

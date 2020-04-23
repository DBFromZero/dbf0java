package dbf0.disk_key_value.readwrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoRunnable;
import dbf0.disk_key_value.readwrite.blocks.*;
import dbf0.disk_key_value.readwrite.btree.*;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchmarkPutThenGet {

  private static final Logger LOGGER = Dbf0Util.getLogger(dbf0.disk_key_value.readonly.Benchmark.class);

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 10);
    Dbf0Util.enableConsoleLogging(Level.INFO);

    var argsItr = Arrays.asList(args).iterator();
    var file = new File(argsItr.next());
    var leafCapacity = Integer.parseInt(argsItr.next());
    var parentCapacity = Integer.parseInt(argsItr.next());
    var keyLength = Integer.parseInt(argsItr.next());
    var valueLength = Integer.parseInt(argsItr.next());
    var keysToWrite = Integer.parseInt(argsItr.next());
    var keySaveFrac = Double.parseDouble(argsItr.next());
    var knownKeyRate = Double.parseDouble(argsItr.next());
    var threadCount = Integer.parseInt(argsItr.next());
    var duration = Duration.parse(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());

    var metadataFile = new File(file.getPath() + "-metadata");
    deleteFile(file);
    deleteFile(metadataFile);

    var config = new BTreeConfig(leafCapacity, parentCapacity);
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
    var baseBtree = new BlockBTree<>(bTreeStorage);
    var btree = new LockingBlockBTree<>(baseBtree, bTreeStorage, () -> false);

    blockStorage.initialize();
    btree.initialize();

    LOGGER.info("writing keys");
    var random = new Random();
    var knownKeys = new ArrayList<ByteArrayWrapper>((int) (keySaveFrac * keysToWrite));
    metadataStore.pauseSync();
    baseBtree.batchPut(IntStream.range(0, keysToWrite).mapToObj(index -> {
      if (index % 10000 == 0) {
        LOGGER.info("writing " + index);
      }
      var key = ByteArrayWrapper.random(random, keyLength);
      if (random.nextDouble() < keySaveFrac) {
        knownKeys.add(key);
      }
      return Pair.of(key, ByteArrayWrapper.random(random, valueLength));
    }));
    metadataStore.resumeSync();

    LOGGER.info("vacuuming");
    metadataStore.pauseSync();
    var vacuum = bTreeStorage.vacuum();
    vacuum.writeNewFile();
    vacuum.commit();
    metadataStore.resumeSync();

    LOGGER.info("starting get");

    var countGet = new AtomicInteger(0);
    var countFound = new AtomicInteger(0);
    var errors = new AtomicInteger(0);

    var threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(IoRunnable.wrap(() -> {
      try {
        var threadRandom = new Random();
        while (!Thread.interrupted()) {
          var known = threadRandom.nextDouble() < knownKeyRate;
          var key = known ? knownKeys.get(threadRandom.nextInt(knownKeys.size())) : ByteArrayWrapper.random(threadRandom, keyLength);
          var value = btree.get(key);
          countGet.incrementAndGet();
          if (value != null) {
            countFound.incrementAndGet();
          }
        }
      } catch (Exception e) {
        errors.incrementAndGet();
        LOGGER.log(Level.SEVERE, e, () -> "failure in thread");
      }
    }))).collect(Collectors.toList());

    threads.forEach(Thread::start);

    var sleepInterval = 250L;
    IntStream.range(0, (int) (duration.toMillis() / sleepInterval)).forEach(ignored -> {
      try {
        if (errors.get() == 0) {
          Thread.sleep(sleepInterval);
        }
        LOGGER.info("stats: " + makeStatsString(countGet, countFound));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    threads.forEach(Thread::interrupt);
    for (var thread : threads) {
      thread.join();
    }

    if (errors.get() > 0) {
      LOGGER.info("Errors were encountered. Not reporting results.");
      System.exit(1);
    }

    System.out.println(makeStatsString(countGet, countFound));
  }

  public static String makeStatsString(AtomicInteger countGet, AtomicInteger countFound) {
    var stats = ImmutableMap.builder()
        .put("get", countGet.get())
        .put("getFound", countFound.get())
        .build();
    return new Gson().toJson(stats);
  }

  public static void deleteFile(File file) {
    if (file.exists()) {
      var deleted = file.delete();
      Preconditions.checkState(deleted);
    }
  }
}

package dbf0.disk_key_value.readwrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileOperationsImpl;
import dbf0.disk_key_value.io.SerializationHelper;
import dbf0.disk_key_value.io.SerializationPair;
import dbf0.disk_key_value.readwrite.blocks.FileBlockStorage;
import dbf0.disk_key_value.readwrite.blocks.FileMetadataStorage;
import dbf0.disk_key_value.readwrite.btree.*;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTree;
import dbf0.test.PutDeleteGet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Benchmark {

  private static final Logger LOGGER = Dbf0Util.getLogger(dbf0.disk_key_value.readonly.Benchmark.class);

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length >= 9);
    Dbf0Util.enableConsoleLogging(Level.INFO, true);

    var argsItr = Arrays.asList(args).iterator();
    var file = new File(argsItr.next());
    var keyLength = Integer.parseInt(argsItr.next());
    var valueLength = Integer.parseInt(argsItr.next());
    var putDeleteGet = PutDeleteGet.valueOf(argsItr.next());
    var knownKeyRate = Double.parseDouble(argsItr.next());
    var threadCount = Integer.parseInt(argsItr.next());
    var duration = Duration.parse(argsItr.next());
    var type = argsItr.next();

    CloseableReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> storage;
    switch (type) {
      case "btree":
        storage = createBTree(argsItr, file);
        break;
      case "lsm":
        storage = createLsmTree(argsItr, file);
        break;
      default:
        throw new IllegalArgumentException("Bad storage type: " + type);
    }
    Preconditions.checkState(!argsItr.hasNext());

    var stats = new Stats();
    var errors = new AtomicInteger(0);
    var threads = IntStream.range(0, threadCount).mapToObj(i ->
        new Thread(() -> runOperations(keyLength, valueLength, putDeleteGet, knownKeyRate, storage, stats, errors)))
        .collect(Collectors.toList());
    threads.forEach(Thread::start);

    waitDuration(duration, stats, errors, file);
    threads.forEach(Thread::interrupt);
    for (var thread : threads) {
      thread.join();
    }
    storage.close();

    if (errors.get() > 0) {
      LOGGER.warning("Errors were encountered. Not reporting results.");
      System.exit(1);
    }

    stats.fileSize.set(file.length());
    System.out.println(stats.toJson());
  }

  private static void waitDuration(Duration duration, Stats stats, AtomicInteger errors, File file) {
    var sleepInterval = 1000L;
    IntStream.range(0, (int) (duration.toMillis() / sleepInterval)).forEach(index -> {
      try {
        if (errors.get() == 0) {
          Thread.sleep(sleepInterval);
        }
        stats.fileSize.set(file.length());
        LOGGER.info(String.format("time=%.1fs size=%s intermediate stats %s",
            (double) (index * sleepInterval) / 1000,
            Dbf0Util.formatBytes(stats.fileSize.get()),
            stats.toJson()));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static LockingBlockBTree<ByteArrayWrapper, ByteArrayWrapper> createBTree(Iterator<String> argsItr, File file)
      throws IOException {
    var leafCapacity = Integer.parseInt(argsItr.next());
    var parentCapacity = Integer.parseInt(argsItr.next());
    var vacuumThreshold = Double.parseDouble(argsItr.next());
    var minVacuumSize = Integer.parseInt(argsItr.next());

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
    var btree = new LockingBlockBTree<>(new BlockBTree<>(bTreeStorage), bTreeStorage, () -> {
      var stats = blockStorage.getStats();
      return stats.unusedBytesFraction() > vacuumThreshold & stats.getUnused().getBytes() > minVacuumSize;
    });

    blockStorage.initialize();
    btree.initialize();
    return btree;
  }

  private static LsmTree<FileOutputStream> createLsmTree(Iterator<String> argsItr, File baseFile) {
    var pendingWritesMergeThreshold = Integer.parseInt(argsItr.next());
    var baseIndexRate = Integer.parseInt(argsItr.next());

    var indexFile = new File(baseFile.getPath() + "-index");
    deleteFile(baseFile);
    deleteFile(indexFile);

    return new LsmTree<>(
        pendingWritesMergeThreshold,
        baseIndexRate,
        new FileOperationsImpl(baseFile, "-tmp"),
        new FileOperationsImpl(indexFile, "-tmp")
    );
  }

  public static void runOperations(int keyLength, int valueLength, PutDeleteGet putDeleteGet, double knownKeyRate,
                                   ReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> storage,
                                   Stats stats, AtomicInteger errors) {
    try {
      var random = new Random();
      var keyTracker = new KeyTracker(random);
      Supplier<ByteArrayWrapper> getDelKeyGenerate = () -> {
        var known = (!keyTracker.isEmpty()) && random.nextDouble() < knownKeyRate;
        return known ? keyTracker.select() : ByteArrayWrapper.random(random, keyLength);
      };
      while (!Thread.interrupted()) {
        var operation = putDeleteGet.select(random);
        switch (operation) {
          case PUT:
            var key = ByteArrayWrapper.random(random, keyLength);
            storage.put(key, ByteArrayWrapper.random(random, valueLength));
            keyTracker.add(key);
            stats.countPut.incrementAndGet();
            break;
          case GET:
            var value = storage.get(getDelKeyGenerate.get());
            stats.countGet.incrementAndGet();
            if (value != null) {
              stats.countFound.incrementAndGet();
            }
            break;
          case DELETE:
            var deleted = storage.delete(getDelKeyGenerate.get());
            stats.countDelete.incrementAndGet();
            if (deleted) {
              stats.countDeleteFound.incrementAndGet();
            }
            break;
          default:
            throw new RuntimeException("Unsupported operation " + operation);
        }
      }
    } catch (Exception e) {
      errors.incrementAndGet();
      LOGGER.log(Level.SEVERE, e, () -> "failure in thread");
    }
  }

  public static void deleteFile(File file) {
    if (file.exists()) {
      var deleted = file.delete();
      Preconditions.checkState(deleted);
    }
  }

  // https://en.wikipedia.org/wiki/Reservoir_sampling
  private static class KeyTracker {
    private final List<ByteArrayWrapper> keys = new ArrayList<>();
    private final int maxKeys = 10 * 1000;
    private final Random random;
    private int keysSeen = 0;

    public KeyTracker(Random random) {
      this.random = random;
    }

    private boolean isEmpty() {
      return keys.isEmpty();
    }

    private void add(ByteArrayWrapper key) {
      if (keys.size() <= maxKeys) {
        keys.add(key);
      } else {
        int i = random.nextInt(keysSeen);
        if (i < keys.size()) {
          keys.set(i, key);
        }
      }
      keysSeen++;
    }

    private ByteArrayWrapper select() {
      return keys.get(random.nextInt(keys.size()));
    }
  }

  private static class Stats {
    private final AtomicLong countPut = new AtomicLong(0);
    private final AtomicLong countDelete = new AtomicLong(0);
    private final AtomicLong countDeleteFound = new AtomicLong(0);
    private final AtomicLong countGet = new AtomicLong(0);
    private final AtomicLong countFound = new AtomicLong(0);
    private final AtomicLong fileSize = new AtomicLong(0);

    private ImmutableMap<String, Long> getMap() {
      return ImmutableMap.<String, Long>builder()
          .put("put", countPut.get())
          .put("delete", countDelete.get())
          .put("deleteFound", countDeleteFound.get())
          .put("get", countGet.get())
          .put("getFound", countFound.get())
          .put("fileSize", fileSize.get())
          .build();
    }

    private String toJson() {
      return new Gson().toJson(getMap());
    }
  }
}

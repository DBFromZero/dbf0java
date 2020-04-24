package dbf0.disk_key_value.readwrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoRunnable;
import dbf0.disk_key_value.io.FileOperationsImpl;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTree;
import dbf0.test.PutDeleteGet;

import java.io.File;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TmpBenchmarkLsm {

  private static final Logger LOGGER = Dbf0Util.getLogger(dbf0.disk_key_value.readonly.Benchmark.class);

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 0);
    Dbf0Util.enableConsoleLogging(Level.INFO);

    var keyLength = 16;
    var valueLength = 1024;
    var putDeleteGet = PutDeleteGet.GET_HEAVY;
    var knownKeyRate = 0.5;
    var threadCount = 8;
    var duration = Duration.ofSeconds(60);

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

    var countPut = new AtomicInteger(0);
    var countDelete = new AtomicInteger(0);
    var countDeleteFound = new AtomicInteger(0);
    var countGet = new AtomicInteger(0);
    var countFound = new AtomicInteger(0);
    var errors = new AtomicInteger(0);

    var threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(IoRunnable.wrap(() -> {
      try {
        var random = new Random();
        var keyTracker = new KeyTracker();
        Supplier<ByteArrayWrapper> getDelKeyGenerate = () -> {
          var known = (!keyTracker.isEmpty()) && random.nextDouble() < knownKeyRate;
          return known ? keyTracker.select(random) : ByteArrayWrapper.random(random, keyLength);
        };
        while (!Thread.interrupted()) {

          var operation = putDeleteGet.select(random);
          switch (operation) {
            case PUT:
              var key = ByteArrayWrapper.random(random, keyLength);
              tree.put(key, ByteArrayWrapper.random(random, valueLength));
              keyTracker.add(key);
              countPut.incrementAndGet();
              break;
            case GET:
              var value = tree.get(getDelKeyGenerate.get());
              countGet.incrementAndGet();
              if (value != null) {
                countFound.incrementAndGet();
              }
              break;
            case DELETE:
              var deleted = tree.delete(getDelKeyGenerate.get());
              countDelete.incrementAndGet();
              if (deleted) {
                countDeleteFound.incrementAndGet();
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
    }))).collect(Collectors.toList());

    threads.forEach(Thread::start);

    var sleepInterval = 250L;
    IntStream.range(0, (int) (duration.toMillis() / sleepInterval)).forEach(ignored -> {
      try {
        if (errors.get() == 0) {
          Thread.sleep(sleepInterval);
        }
        LOGGER.info(getJson(countPut, countDelete, countDeleteFound, countGet, countFound));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    threads.forEach(Thread::interrupt);
    for (var thread : threads) {
      thread.join();
    }

    if (errors.get() > 0) {
      LOGGER.warning("Errors were encountered. Not reporting results.");
      System.exit(1);
    }

    String json = getJson(countPut, countDelete, countDeleteFound, countGet, countFound);
    System.out.println(json);
  }

  public static String getJson(AtomicInteger countPut, AtomicInteger countDelete, AtomicInteger countDeleteFound, AtomicInteger countGet, AtomicInteger countFound) {
    var stats = ImmutableMap.builder()
        .put("put", countPut.get())
        .put("delete", countDelete.get())
        .put("deleteFound", countDeleteFound.get())
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

  private static class KeyTracker {
    private final LinkedList<ByteArrayWrapper> keys = new LinkedList<>();
    private final int maxKeys = 100;

    private boolean isEmpty() {
      return keys.isEmpty();
    }

    private void add(ByteArrayWrapper key) {
      keys.add(key);
      if (keys.size() == maxKeys) {
        keys.removeFirst();
      }
    }

    private ByteArrayWrapper select(Random r) {
      return keys.stream().skip(r.nextInt(keys.size())).findFirst().get();
    }
  }
}

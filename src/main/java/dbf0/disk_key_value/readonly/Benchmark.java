package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoRunnable;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Benchmark {

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 7);
    Dbf0Util.enableConsoleLogging(Level.INFO);

    var dataPath = args[0];
    var indexPath = args[1];
    var knownKeysDataPath = args[2];
    var keySetSize = Integer.parseInt(args[3]);
    var knownKeyGetFrac = Float.parseFloat(args[4]);
    var readThreads = Integer.parseInt(args[5]);
    var duration = Duration.parse(args[6]);

    var store = new ReadOnlyKeyValueStorage(dataPath, ReadOnlyKeyValueStorage.readIndex(indexPath));
    var random = new Random();

    var knownKeys = Dbf0Util.iteratorStream(new KeyOnlyFileIterator(new KeyValueFileReader(knownKeysDataPath)))
        .limit(keySetSize)
        .collect(Collectors.toList());
    if (knownKeys.size() < keySetSize) {
      throw new RuntimeException("Insufficient known keys. Only " + knownKeys.size() +
          " and require " + keySetSize);
    }
    Collections.shuffle(knownKeys, random);

    var unknownKeys = IntStream.range(0, keySetSize).mapToObj(i ->
        ByteArrayWrapper.random(random, WriteSortedKeyValueFiles.KEY_LENGTH))
        .collect(Collectors.toList());

    var countGet = new AtomicInteger(0);
    var countFound = new AtomicInteger(0);

    var threads = IntStream.range(0, readThreads).mapToObj(i -> new Thread(IoRunnable.wrap(() -> {
      var threadRandom = new Random();
      while (!Thread.interrupted()) {
        var keys = threadRandom.nextFloat() < knownKeyGetFrac ? knownKeys : unknownKeys;
        var key = keys.get(threadRandom.nextInt(keys.size()));
        boolean found = store.get(key) != null;
        countGet.incrementAndGet();
        if (found) {
          countFound.incrementAndGet();
        }
      }
    }))).collect(Collectors.toList());

    threads.forEach(Thread::start);
    Thread.sleep(duration.toMillis());
    threads.forEach(Thread::interrupt);

    for (var thread : threads) {
      thread.join();
    }

    var stats = ImmutableMap.of("get", countGet.get(), "found", countFound.get());
    System.out.println(new Gson().toJson(stats));
  }
}

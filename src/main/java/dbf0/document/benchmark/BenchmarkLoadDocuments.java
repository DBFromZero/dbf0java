package dbf0.document.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.HashPartitionedReadWriteStorage;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;
import dbf0.disk_key_value.readwrite.ReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTree;
import dbf0.document.gson.DElementTypeAdapter;
import dbf0.document.types.DElement;
import dbf0.document.types.DMap;
import dbf0.document.types.DString;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchmarkLoadDocuments {
  private static final Logger LOGGER = Dbf0Util.getLogger(BenchmarkLoadDocuments.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var directory = new File(argsItr.next());
    var pendingWritesMergeThreshold = Integer.parseInt(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    var partitions = Integer.parseInt(argsItr.next());
    var writingThreadsCount = Integer.parseInt(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());

    var base = new FileDirectoryOperationsImpl(directory);
    base.mkdirs();
    base.clear();

    var executor = Executors.newScheduledThreadPool(8);
    var store = new ReadWriteStorageWithBackgroundTasks<>(
        HashPartitionedReadWriteStorage.create(partitions,
            partition -> createLsmTree(base.subDirectory(String.valueOf(partition)),
                pendingWritesMergeThreshold, indexRate, executor)),
        executor);
    store.initialize();

    var errors = new AtomicInteger(0);
    var readDone = new AtomicBoolean(false);
    var writes = new AtomicLong(0);
    var queue = new LinkedBlockingQueue<String>(100);

    var startTime = System.nanoTime();
    var reportFuture = executor.scheduleWithFixedDelay(() -> report(errors, writes, directory, startTime),
        0, 1, TimeUnit.SECONDS);

    var readThread = new Thread(() -> readQueue(errors, queue));
    readThread.start();

    var writeThreads = IntStream.range(0, writingThreadsCount)
        .mapToObj(ignored -> new Thread(() -> write(errors, readDone, writes, queue, store)))
        .collect(Collectors.toList());
    writeThreads.forEach(Thread::start);

    var threads = new ArrayList<>(writeThreads);
    threads.add(readThread);

    while (errors.get() == 0 && readThread.isAlive()) {
      readThread.join(1000L);
    }
    readDone.set(true);

    if (errors.get() != 0) {
      for (Thread thread : threads) {
        if (thread.isAlive()) {
          thread.interrupt();
        }
      }
    }

    for (Thread thread : threads) {
      thread.join();
    }

    reportFuture.cancel(false);
    store.close();
    report(errors, writes, directory, startTime);
    System.exit(errors.get() == 0 ? 0 : 1);
  }

  private static LsmTree<FileOutputStream, DElement, DElement>
  createLsmTree(FileDirectoryOperationsImpl directoryOperations,
                int pendingWritesMergeThreshold, int indexRate,
                ScheduledExecutorService executorService) throws IOException {
    directoryOperations.mkdirs();
    directoryOperations.clear();

    return LsmTree.<FileOutputStream>builderForDocuments()
        .withBaseDeltaFiles(directoryOperations)
        .withPendingWritesDeltaThreshold(pendingWritesMergeThreshold)
        .withScheduledExecutorService(executorService)
        .withIndexRate(indexRate)
        .withMaxInFlightWriteJobs(20)
        .withMaxDeltaReadPercentage(0.75)
        .withMergeCronFrequency(Duration.ofSeconds(1))
        .build();
  }

  private static void readQueue(AtomicInteger errors, BlockingQueue<String> queue) {
    try {
      var reader = new BufferedReader(new InputStreamReader(System.in), 0x8000);
      while (errors.get() == 0) {
        var line = reader.readLine();
        if (line == null) {
          break;
        }
        queue.put(line);
      }
    } catch (Exception e) {
      errors.incrementAndGet();
      LOGGER.log(Level.SEVERE, e, () -> "Error in reading input");
    }
  }

  private static void write(AtomicInteger errors, AtomicBoolean readDone, AtomicLong writes,
                            BlockingQueue<String> queue,
                            ReadWriteStorage<DElement, DElement> store) {
    var adapter = DElementTypeAdapter.getInstance();
    var idKey = DString.of("id");
    try {
      while (errors.get() == 0) {
        var line = queue.poll(100, TimeUnit.MILLISECONDS);
        if (line == null) {
          if (readDone.get()) {
            break;
          }
          continue;
        }
        var element = (DMap) adapter.fromJson(line);
        var id = (DString) element.getEntries().get(idKey);
        store.put(id, element);
        writes.incrementAndGet();
      }
    } catch (InterruptedException ignored) {
    } catch (Exception e) {
      errors.incrementAndGet();
      LOGGER.log(Level.SEVERE, e, () -> "Error in writing");
    }
  }

  private static void report(AtomicInteger errors, AtomicLong writes,
                             File directory, long startTime) {
    if (errors.get() != 0) {
      return;
    }
    var time = System.nanoTime();
    var size = fileSize(directory);
    var writesValue = writes.get();
    var stats = ImmutableMap.<String, Object>builder()
        .put("time", time)
        .put("writes", writesValue)
        .put("size", size)
        .build();
    System.out.println(stats);
    LOGGER.info(String.format("%s writes=%.1e size=%s",
        Duration.ofNanos(time - startTime),
        (double) writesValue,
        Dbf0Util.formatBytes(size)));
  }

  static long fileSize(File f) {
    try {
      if (f.isFile()) {
        return f.length();
      }
      Preconditions.checkState(f.isDirectory());
      return Arrays.stream(f.listFiles()).mapToLong(BenchmarkLoadDocuments::fileSize).sum();
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, e, () -> "Error in getting file size for " + f.getPath());
      return 0;
    }
  }
}

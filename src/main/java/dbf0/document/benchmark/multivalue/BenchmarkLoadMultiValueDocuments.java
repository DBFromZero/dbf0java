package dbf0.document.benchmark.multivalue;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.HashPartitionedMultiValueReadWriteStorage;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.MultiValueLsmTree;
import dbf0.document.gson.DElementTypeAdapter;
import dbf0.document.types.DElement;
import dbf0.document.types.DMap;
import dbf0.document.types.DString;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

import static dbf0.document.benchmark.singlevalue.BenchmarkLoadDocuments.readQueue;
import static dbf0.document.benchmark.singlevalue.BenchmarkLoadDocuments.report;

public class BenchmarkLoadMultiValueDocuments {
  private static final Logger LOGGER = Dbf0Util.getLogger(BenchmarkLoadMultiValueDocuments.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var readFile = new File(argsItr.next());
    var directory = new File(argsItr.next());
    var idKey = argsItr.next();
    var pendingWritesMergeThreshold = Integer.parseInt(argsItr.next());
    var indexRate = Integer.parseInt(argsItr.next());
    var partitions = Integer.parseInt(argsItr.next());
    var readingThreadCount = Integer.parseInt(argsItr.next());
    var writingThreadsCount = Integer.parseInt(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());

    var base = new FileDirectoryOperationsImpl(directory);
    base.mkdirs();
    base.clear();

    var executor = Executors.newScheduledThreadPool(30);
    var store = new MultiValueReadWriteStorageWithBackgroundTasks<>(
        createStore(pendingWritesMergeThreshold, indexRate, partitions, base, executor),
        executor);
    store.initialize();

    var errors = new AtomicInteger(0);
    var readDone = new AtomicBoolean(false);
    var writes = new AtomicLong(0);
    var queue = new LinkedBlockingQueue<String>(100);

    var startTime = System.nanoTime();
    var reportFuture = executor.scheduleWithFixedDelay(() -> report(errors, writes, directory, startTime),
        0, 1, TimeUnit.SECONDS);

    var readOffset = readFile.length() / readingThreadCount;

    var readThreads = IntStream.range(0, readingThreadCount)
        .mapToObj(i -> new Thread(() -> readQueue(errors, readFile, i * readOffset,
            (i + 1) * readOffset, queue), "read-" + i))
        .collect(Collectors.toList());
    readThreads.forEach(Thread::start);

    var writeThreads = IntStream.range(0, writingThreadsCount)
        .mapToObj(i -> new Thread(() -> write(errors, readDone, writes, idKey, queue, store), "write-" + i))
        .collect(Collectors.toList());
    writeThreads.forEach(Thread::start);

    var threads = new ArrayList<>(readThreads);
    threads.addAll(writeThreads);

    while (errors.get() == 0 && readThreads.stream().anyMatch(Thread::isAlive)) {
      for (Thread readThread : readThreads) {
        if (readThread.isAlive()) {
          readThread.join(200L);
        }
      }
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

  @NotNull static HashPartitionedMultiValueReadWriteStorage<DElement, DElement>
  createStore(int pendingWritesMergeThreshold, int indexRate, int partitions,
              FileDirectoryOperationsImpl base, ScheduledExecutorService executor) throws IOException {
    return HashPartitionedMultiValueReadWriteStorage.create(partitions,
        partition -> createLsmTree(base.subDirectory(String.valueOf(partition)),
            pendingWritesMergeThreshold, indexRate, executor));
  }

  @NotNull static MultiValueLsmTree<FileOutputStream, DElement, DElement>
  createLsmTree(FileDirectoryOperationsImpl directoryOperations,
                int pendingWritesMergeThreshold, int indexRate,
                ScheduledExecutorService executorService) throws IOException {
    directoryOperations.mkdirs();

    return MultiValueLsmTree.<FileOutputStream>builderForDocuments(
        LsmTreeConfiguration.builderForMultiValueDocuments()
            .withPendingWritesDeltaThreshold(pendingWritesMergeThreshold)
            .withIndexRate(indexRate)
            .withMaxInFlightWriteJobs(3)
            .withMaxDeltaReadPercentage(0.75)
            .withMergeCronFrequency(Duration.ofSeconds(1))
            .build())
        .withBaseDeltaFiles(directoryOperations)
        .withScheduledExecutorService(executorService)
        .build();
  }

  private static void write(AtomicInteger errors, AtomicBoolean readDone, AtomicLong writes, String idKey,
                            BlockingQueue<String> queue,
                            MultiValueReadWriteStorage<DElement, DElement> store) {
    var adapter = DElementTypeAdapter.getInstance();
    var dIdKey = DString.of(idKey);
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
        var id = element.getEntries().get(dIdKey);
        Preconditions.checkState(id != null, "no such id %s in %s", id, element);
        store.put(id, element);
        writes.incrementAndGet();
      }
    } catch (InterruptedException ignored) {
    } catch (Exception e) {
      errors.incrementAndGet();
      LOGGER.log(Level.SEVERE, e, () -> "Error in writing");
    }
  }
}

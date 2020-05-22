package dbf0.document.benchmark.multivalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.ParallelLineReader;
import dbf0.common.ParallelThreads;
import dbf0.document.gson.DElementTypeAdapter;
import dbf0.document.serialization.DElementSerializer;
import dbf0.document.types.DElement;
import dbf0.document.types.DMap;
import dbf0.document.types.DString;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static dbf0.document.benchmark.singlevalue.BenchmarkLoadDocuments.fileSize;

public class ParseAndPruneDocuments {
  private static final Logger LOGGER = Dbf0Util.getLogger(ParseAndPruneDocuments.class);
  private static final List<DString> ATTRIBUTES = List.of(DString.of("id"), DString.of("subreddit"),
      DString.of("author"));

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);

    var argsItr = Arrays.asList(args).iterator();
    var readFile = new File(argsItr.next());
    var outputFile = new File(argsItr.next());
    var readingThreadCount = Integer.parseInt(argsItr.next());
    var parseAndSerializeThreadCount = Integer.parseInt(argsItr.next());
    Preconditions.checkState(!argsItr.hasNext());

    int queueCount = Math.max(1, readingThreadCount / 2);
    LOGGER.info("Using " + queueCount + " queues");

    var error = new AtomicBoolean(false);
    var readDone = new AtomicBoolean(false);
    var writes = new AtomicLong();
    long startTime = System.nanoTime();

    var executorService = Executors.newSingleThreadScheduledExecutor();
    var reportFuture = executorService.scheduleWithFixedDelay(() -> report(error, writes, outputFile, startTime),
        1, 1, TimeUnit.SECONDS);

    try (var parallelLineReader = new ParallelLineReader(readFile, readingThreadCount, queueCount, 256, error)) {
      parallelLineReader.start();

      var serialized = new LinkedBlockingQueue<ByteArrayWrapper>(1000);

      try (var parseThreads = ParallelThreads.create(error, parseAndSerializeThreadCount,
          i -> new Thread(() -> parse(error, readDone, parallelLineReader.getQueueForIndex(i), serialized),
              "parse-" + i))) {
        parseThreads.start();

        try (var writeThread = new ParallelThreads(error, List.of(
            new Thread(() -> write(error, readDone, writes, serialized, outputFile), "write")))) {
          writeThread.start();

          parallelLineReader.awaitCompletion();
          readDone.set(true);
          if (error.get()) {
            parallelLineReader.abort();
            parseThreads.abort();
            writeThread.abort();
          } else {
            parseThreads.awaitCompletion();
            writeThread.awaitCompletion();
          }
        }
      }
    }
    reportFuture.cancel(false);

    report(error, writes, outputFile, startTime);
    System.exit(error.get() ? 1 : 0);
  }


  public static void report(AtomicBoolean errors, AtomicLong writes,
                            File directory, long startTime) {
    if (errors.get()) {
      return;
    }
    var time = System.nanoTime();
    var size = fileSize(directory);
    var writesValue = writes.get();
    LOGGER.info(String.format("%s writes=%.3e size=%s",
        Duration.ofNanos(time - startTime),
        (double) writesValue,
        Dbf0Util.formatBytes(size)));
  }

  private static void parse(AtomicBoolean error, AtomicBoolean readDone,
                            BlockingQueue<String> inputQueue, LinkedBlockingQueue<ByteArrayWrapper> outputQueue) {
    var adapter = DElementTypeAdapter.getInstance();
    var serializer = DElementSerializer.defaultCharsetInstance();
    var buffer = new ByteArrayOutputStream(1024);
    try {
      while (!error.get()) {
        var line = inputQueue.poll(100, TimeUnit.MILLISECONDS);
        if (line == null) {
          if (readDone.get()) {
            break;
          }
          continue;
        }
        var element = (DMap) adapter.fromJson(line);
        var entries = element.getEntries();
        var builder = ImmutableMap.<DElement, DElement>builder();
        for (DString attr : ATTRIBUTES) {
          builder.put(attr, Preconditions.checkNotNull(entries.get(attr),
              "no such attr %s in %s", attr, element));
        }
        var pruned = DMap.of(builder.build());
        buffer.reset();
        serializer.serialize(buffer, pruned);
        outputQueue.put(ByteArrayWrapper.of(buffer.toByteArray()));
      }
    } catch (InterruptedException ignored) {
    } catch (Exception e) {
      error.set(true);
      LOGGER.log(Level.SEVERE, e, () -> "Error in parsing");
    }
  }

  private static void write(AtomicBoolean error, AtomicBoolean readDone, AtomicLong writes,
                            LinkedBlockingQueue<ByteArrayWrapper> queue, File file) {
    try (var stream = new BufferedOutputStream(new FileOutputStream(file), 0x8000)) {
      while (!error.get()) {
        var entry = queue.poll(100, TimeUnit.MILLISECONDS);
        if (entry == null) {
          if (readDone.get()) {
            break;
          }
        } else {
          stream.write(entry.getArray());
          writes.getAndIncrement();
        }
      }
    } catch (InterruptedException ignored) {
    } catch (Exception e) {
      error.set(true);
      LOGGER.log(Level.SEVERE, e, () -> "Error in parsing");
    }
  }
}

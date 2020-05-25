package dbf0.common;

import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ParallelLineReader implements Closeable {

  private static final Logger LOGGER = Dbf0Util.getLogger(ParallelLineReader.class);


  private final File file;
  private final int threadCount;
  private final AtomicBoolean error;

  private final List<LinkedBlockingQueue<String>> queues;
  private final List<FileChannel> channels;
  private ParallelThreads threads;


  public ParallelLineReader(File file, int threadCount, int queueCount, int queueSize, AtomicBoolean error) {
    Preconditions.checkArgument(threadCount > 0);
    Preconditions.checkArgument(queueCount > 0);
    Preconditions.checkArgument(threadCount >= queueCount);
    Preconditions.checkArgument(queueSize > 0);

    this.file = Preconditions.checkNotNull(file);
    this.threadCount = threadCount;
    this.error = Preconditions.checkNotNull(error);

    queues = Stream.generate(() -> new LinkedBlockingQueue<String>(queueSize)).limit(queueCount)
        .collect(Collectors.toList());
    channels = new ArrayList<>(threadCount);
  }

  public boolean isError() {
    return error.get();
  }

  public void setError() {
    error.set(true);
  }

  public LinkedBlockingQueue<String> getQueueForIndex(int index) {
    Preconditions.checkArgument(index >= 0);
    return queues.get(index % queues.size());
  }

  public void start() {
    Preconditions.checkState(threads == null, "already started");
    Preconditions.checkState(file.exists());
    Preconditions.checkState(file.isFile());

    var readOffset = file.length() / threadCount;
    threads = ParallelThreads.create(error, threadCount, i -> new Thread(() ->
        readQueue(i * readOffset, (i + 1) * readOffset, getQueueForIndex(i)), "read-" + i));
    threads.start();
  }

  public void awaitCompletion() throws InterruptedException {
    threads.awaitCompletion();
  }

  public void abort() throws IOException {
    Preconditions.checkState(threads != null, "not started");
    setError();
    close();
  }

  @Override public void close() throws IOException {
    List<FileChannel> cs;
    synchronized (channels) {
      cs = new ArrayList<>(channels);
      channels.clear();
    }
    var errors = new ArrayList<Exception>();
    for (FileChannel channel : cs) {
      try {
        channel.close();
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Error closing channel", e);
        errors.add(e);
      }
    }

    if (threads != null) {
      var ts = threads;
      threads = null;
      try {
        ts.close();
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Error closing threads", e);
        errors.add(e);
      }
    }

    if (!errors.isEmpty()) {
      var firstError = errors.remove(0);
      var combined = new IOException("Error closing channel", firstError);
      for (var subsequentError : errors) {
        combined.addSuppressed(subsequentError);
      }
      throw combined;
    }
  }

  private void readQueue(long start, long end, BlockingQueue<String> queue) {
    try (var channel = FileChannel.open(file.toPath())) {
      synchronized (channels) {
        channels.add(channel);
      }
      var result = channel.position(start);
      Preconditions.checkState(result == channel);
      var reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
      if (start != 0) {
        reader.readLine();
      }
      int i = 0;
      while (!error.get()) {
        if (i++ % 10 == 0 && channel.position() > end) {
          break;
        }
        var line = reader.readLine();
        if (line == null) {
          break;
        }
        queue.put(line);
      }
    } catch (InterruptedException ignored) {
    } catch (ClosedChannelException e) {
      if (!isError()) {
        setError();
        LOGGER.log(Level.SEVERE, "Channel closed unexpectedly", e);
      }
    } catch (Exception e) {
      setError();
      LOGGER.log(Level.SEVERE, "Error in reading input", e);
    }
  }
}

package dbf0.common;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class ParallelThreads implements Closeable {

  private static final Logger LOGGER = Dbf0Util.getLogger(ParallelThreads.class);

  private enum State {
    NOT_STARTED,
    RUNNING,
    FINISHED
  }

  private final AtomicBoolean error;
  private final List<Thread> threads;
  private State state = State.NOT_STARTED;

  public ParallelThreads(AtomicBoolean error, Collection<Thread> threads) {
    this.error = Preconditions.checkNotNull(error);
    this.threads = new ArrayList<>(Preconditions.checkNotNull(threads));
    var invalidThreadState = this.threads.stream().map(Thread::getState).filter(s -> s != Thread.State.NEW)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    if (!invalidThreadState.isEmpty()) {
      throw new IllegalArgumentException("Invalid thread states: " +
          Joiner.on(", ").join(invalidThreadState.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
              .iterator()));
    }
  }

  public static <X> ParallelThreads create(AtomicBoolean error, Stream<X> stream, Function<X, Thread> factory) {
    return new ParallelThreads(error, stream.map(factory).collect(Collectors.toList()));
  }

  public static <X> ParallelThreads create(AtomicBoolean error, Collection<X> collection, Function<X, Thread> factory) {
    return create(error, collection.stream(), factory);
  }

  public static ParallelThreads create(AtomicBoolean error, int count, Function<Integer, Thread> factory) {
    return create(error, IntStream.range(0, count).boxed(), factory);
  }


  public boolean isError() {
    return error.get();
  }

  public void setError() {
    error.set(true);
  }

  public void start() {
    Preconditions.checkState(state == State.NOT_STARTED);
    threads.forEach(Thread::start);
    state = State.RUNNING;
  }

  public boolean hasLiveThreads() {
    return state == State.RUNNING && threads.stream().anyMatch(Thread::isAlive);
  }

  public void joinLiveThreads(Duration timeout) throws InterruptedException {
    if (state == State.FINISHED) {
      return;
    }
    Preconditions.checkState(state == State.RUNNING);
    var millis = timeout.toMillis();
    for (Thread readThread : threads) {
      if (readThread.isAlive()) {
        readThread.join(millis);
      }
    }
  }

  public void awaitCompletion() throws InterruptedException {
    var timeout = Duration.ofMillis(200);
    while (!isError() && hasLiveThreads()) {
      joinLiveThreads(timeout);
    }
    close();
  }

  public void abort() {
    if (state == State.FINISHED) {
      return;
    }
    Preconditions.checkState(state == State.RUNNING);
    setError();
    close();
  }

  @Override public void close() {
    if (state == State.FINISHED) {
      return;
    }
    Preconditions.checkState(state == State.RUNNING);
    state = State.FINISHED;

    if (isError()) {
      threads.stream().filter(Thread::isAlive).forEach(Thread::interrupt);
    }

    boolean interrupted = false;
    for (var thread : threads) {
      for (int attempt = 0; attempt < 5; attempt++) {
        try {
          thread.join();
          break;
        } catch (InterruptedException e) {
          LOGGER.log(Level.WARNING, "Interrupted in joining thread on attempt " + attempt, e);
          interrupted = true;
        }
      }
    }
    threads.clear();
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in joining thread");
    }
  }
}

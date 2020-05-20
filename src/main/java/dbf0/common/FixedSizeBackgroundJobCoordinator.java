package dbf0.common;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class FixedSizeBackgroundJobCoordinator<X extends Runnable> {

  private final ExecutorService executor;
  private final int maxInFlightJobs;

  private final LinkedList<X> inFlightJobs = new LinkedList<>();

  public FixedSizeBackgroundJobCoordinator(ExecutorService executor, int maxInFlightJobs) {
    Preconditions.checkArgument(maxInFlightJobs > 0);
    this.executor = executor;
    this.maxInFlightJobs = maxInFlightJobs;
  }

  public synchronized boolean hasMaxInFlightJobs() {
    return inFlightJobs.size() == maxInFlightJobs;
  }

  public synchronized boolean hasInFlightJobs() {
    return !inFlightJobs.isEmpty();
  }

  public synchronized List<X> getCurrentInFlightJobs() {
    return new ArrayList<>(inFlightJobs);
  }

  public synchronized void execute(X job) {
    Preconditions.checkState(!hasMaxInFlightJobs());
    executeInternal(job);
  }

  private void executeInternal(X job) {
    inFlightJobs.add(job);
    executor.execute(() -> run(job));
  }

  public synchronized void awaitNextJobCompletion() throws InterruptedException {
    if (!inFlightJobs.isEmpty()) {
      wait();
    }
  }

  public void waitUntilExecute(X job) throws InterruptedException {
    while (true) {
      synchronized (this) {
        if (!hasMaxInFlightJobs()) {
          executeInternal(job);
          break;
        }
        wait();
      }
    }
  }

  private void run(X job) {
    try {
      job.run();
    } finally {
      synchronized (this) {
        var removed = inFlightJobs.remove(job);
        Preconditions.checkState(removed);
        notify();
      }
    }
  }
}

package dbf0.disk_key_value.readwrite.log;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileOperations;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FrequencyLogSynchronizer<T extends OutputStream> implements LogSynchronizer {

  private static final Logger LOGGER = Dbf0Util.getLogger(FrequencyLogSynchronizer.class);

  private final FileOperations<T> fileOperations;
  private final T outputStream;

  private ScheduledFuture<?> checkingFuture;
  private boolean dirty = false;
  private boolean errorInSync = false;

  public FrequencyLogSynchronizer(FileOperations<T> fileOperations, T outputStream) {
    this.fileOperations = fileOperations;
    this.outputStream = outputStream;
  }

  public synchronized void schedule(ScheduledExecutorService scheduledExecutorService, Duration frequency) {
    Preconditions.checkState(checkingFuture == null);
    checkingFuture = scheduledExecutorService.scheduleWithFixedDelay(this::synchronizeRunnable,
        frequency.toMillis(), frequency.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override public void registerLog() throws IOException {
    Preconditions.checkState(checkingFuture != null, "not initialized");
    if (errorInSync) {
      throw new RuntimeException("error in synchronizing and write-ahead-log is no longer usable");
    }
    dirty = true;
  }

  @Override public synchronized void close() throws IOException {
    if (checkingFuture != null) {
      checkingFuture.cancel(false);
      checkingFuture = null;
      synchronize();
    }
  }

  private void synchronize() throws IOException {
    if (dirty) {
      fileOperations.sync(outputStream);
      dirty = false;
    }
  }

  private synchronized void synchronizeRunnable() {
    try {
      synchronize();
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "error in synchronizing", e);
      errorInSync = true;
      checkingFuture.cancel(false);
      checkingFuture = null;
    }
  }

  public static class Factory<T extends OutputStream> implements LogSynchronizer.Factory<T> {

    private final ScheduledExecutorService scheduledExecutorService;
    private final Duration frequency;

    public Factory(ScheduledExecutorService scheduledExecutorService, Duration frequency) {
      this.scheduledExecutorService = scheduledExecutorService;
      this.frequency = frequency;
    }

    @Override public LogSynchronizer create(FileOperations<T> fileOperations, T outputStream) throws IOException {
      var synchronizer = new FrequencyLogSynchronizer<T>(fileOperations, outputStream);
      synchronizer.schedule(scheduledExecutorService, frequency);
      return synchronizer;
    }
  }

  public static <T extends OutputStream> LogSynchronizer.Factory<T> factory(
      ScheduledExecutorService scheduledExecutorService, Duration frequency) {
    return new Factory<>(scheduledExecutorService, frequency);
  }
}

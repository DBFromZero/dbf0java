package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.InterruptedExceptionWrapper;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.readonly.base.BaseRandomAccessKeyValueFileReader;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

public abstract class BaseLsmTree<T extends OutputStream, K, V, W, P extends PendingWrites<W>,
    R extends BaseRandomAccessKeyValueFileReader<K, V, ?>> implements Closeable {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseLsmTree.class);

  protected final LsmTreeConfiguration<K, V> configuration;
  protected final BaseDeltaFiles<T, K, V, R> baseDeltaFiles;
  protected final WriteJobCoordinator<T, W, P> coordinator;
  protected final BaseDeltaMergerCron<T> mergerCron;

  protected final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  protected P pendingWrites;

  protected BaseLsmTree(LsmTreeConfiguration<K, V> configuration,
                        BaseDeltaFiles<T, K, V, R> baseDeltaFiles,
                        WriteJobCoordinator<T, W, P> coordinator,
                        BaseDeltaMergerCron<T> mergerCron) {
    this.configuration = configuration;
    this.baseDeltaFiles = baseDeltaFiles;
    this.coordinator = coordinator;
    this.mergerCron = mergerCron;
  }

  abstract protected P createNewPendingWrites() throws IOException;

  abstract protected void sendWritesToCoordinator() throws IOException;

  protected void initialize() throws IOException {
    baseDeltaFiles.loadExisting();
    checkForExisting();
    mergerCron.start();
    pendingWrites = createNewPendingWrites();
  }

  protected void checkForExisting() throws IOException {
  }

  @Override public void close() throws IOException {
    mergerCron.shutdown();
    if (isUsable()) {
      lock.runWithWriteLock(() -> {
        if (pendingWrites == null) {
          return;
        }
        if (!pendingWrites.isEmpty()) {
          LOGGER.info("writing pending at close");
          if (coordinator.hasMaxInFlightWriters()) {
            LOGGER.warning("Delaying close because DeltaWriteCoordinator is backed up");
            waitForWritesToUnblock();
          } else {
            sendWritesToCoordinator();
          }
        }
        pendingWrites = null;
        try {
          while (coordinator.hasInFlightWriters() && coordinator.isUsable()) {
            LOGGER.info("Waiting for deltas to finish writing");
            coordinator.awaitNextJobCompletion();
          }
        } catch (InterruptedException e) {
          var msg = "Interrupted waiting for deltas to finish writing";
          LOGGER.warning(msg);
          throw new InterruptedExceptionWrapper(msg, e);
        }
      });
    }
  }

  public boolean isUsable() {
    return coordinator.isUsable() && !mergerCron.hasErrors();
  }

  public void waitForAllDeltasToMerge() throws InterruptedException {
    Preconditions.checkState(isUsable());
    mergerCron.waitForAllDeltasToMerge();
  }

  @Nullable protected V checkForDeleteValue(@NotNull V value) {
    return value.equals(configuration.getDeleteValue()) ? null : value;
  }

  /**
   * only to be called when holding the write lock
   *
   * @return whether the caller needs to call {@code waitForWritesToUnblock} after
   * releasing the lock
   */
  protected boolean checkMergeThreshold() throws IOException {
    if (pendingWrites.size() < configuration.getPendingWritesDeltaThreshold()) {
      return false;
    }
    if (coordinator.hasMaxInFlightWriters()) {
      return true;
    }
    sendWritesToCoordinator();
    pendingWrites = createNewPendingWrites();
    return false;
  }

  // for the future it would be nice if we could reject writes and let the application decide how to proceed
  protected void waitForWritesToUnblock() throws IOException {
    LOGGER.warning("There are too many in-flight write jobs. Waiting for them to finish");
    try {
      boolean exit;
      do {
        while (coordinator.hasMaxInFlightWriters() && coordinator.isUsable()) {
          coordinator.awaitNextJobCompletion();
        }
        exit = lock.callWithWriteLock(() -> {
          if (!coordinator.isUsable()) {
            return true;
          }
          if (coordinator.hasMaxInFlightWriters()) {
            return false;
          }
          sendWritesToCoordinator();
          pendingWrites = createNewPendingWrites();
          return true;
        });
      } while (!exit);
    } catch (InterruptedException e) {
      throw new InterruptedExceptionWrapper("interrupted waiting for write jobs to finish", e);
    }
  }
}

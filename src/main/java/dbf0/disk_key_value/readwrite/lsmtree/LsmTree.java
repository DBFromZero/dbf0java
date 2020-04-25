package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.readwrite.CloseableReadWriteStorage;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class LsmTree<T extends OutputStream> implements CloseableReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper> {

  private static final Logger LOGGER = Dbf0Util.getLogger(LsmTree.class);
  static final ByteArrayWrapper DELETE_VALUE = ByteArrayWrapper.of(
      83, 76, 69, 7, 95, 21, 81, 27, 2, 104, 8, 100, 45, 109, 110, 1);

  private final int pendingWritesMergeThreshold;
  private final BaseDeltaFiles<T> baseDeltaFiles;
  private final DeltaWriterCoordinator<T> coordinator;
  private final BaseDeltaMergerCron<T> mergerCron;

  private final ReadWriteLockHelper lock = new ReadWriteLockHelper();
  private Map<ByteArrayWrapper, ByteArrayWrapper> pendingWrites = new HashMap<>();

  public LsmTree(int pendingWritesMergeThreshold,
                 BaseDeltaFiles<T> baseDeltaFiles,
                 DeltaWriterCoordinator<T> coordinator,
                 BaseDeltaMergerCron<T> mergerCron) {
    Preconditions.checkArgument(pendingWritesMergeThreshold > 0);
    this.pendingWritesMergeThreshold = pendingWritesMergeThreshold;
    this.baseDeltaFiles = baseDeltaFiles;
    this.coordinator = coordinator;
    this.mergerCron = mergerCron;
  }

  public void initialize() throws IOException {
    mergerCron.start();
  }

  @Override public void close() throws IOException {
    mergerCron.shutdown();
    if (isUsable()) {
      lock.runWithWriteLock(() -> {
        if (!pendingWrites.isEmpty()) {
          if (coordinator.hasMaxInFlightWriters()) {
            LOGGER.warning("Delaying close because DeltaWriteCoordinator is backed up");
            waitForWritesToUnblock();
          }
          sendWritesToCoordinator();
        }
        pendingWrites = null;
      });
    }
  }


  @Override public int size() throws IOException {
    throw new RuntimeException("not implemented");
  }

  public boolean isUsable() {
    return !coordinator.anyWritesAborted() && !mergerCron.hasErrors();
  }

  @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      pendingWrites.put(key, value);
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
  }

  @Nullable @Override public ByteArrayWrapper get(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(isUsable());
    var outerValue = lock.callWithReadLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      var value = pendingWrites.get(key);
      if (value != null) {
        return value;
      }
      return coordinator.searchForKeyInWritesInProgress(key);
    });
    if (outerValue != null) {
      return outerValue.equals(DELETE_VALUE) ? null : outerValue;
    }
    return baseDeltaFiles.hasInUseBase() ? baseDeltaFiles.searchForKey(key) : null;
  }

  @Override public boolean delete(@NotNull ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(isUsable());
    var writesBlocked = lock.callWithWriteLock(() -> {
      Preconditions.checkState(pendingWrites != null, "is closed");
      pendingWrites.put(key, DELETE_VALUE);
      return checkMergeThreshold();
    });
    if (writesBlocked) {
      waitForWritesToUnblock();
    }
    return true;// doesn't actually return a useful value
  }

  // only to be called when holding the write lock
  private boolean checkMergeThreshold() throws IOException {
    if (pendingWrites.size() < pendingWritesMergeThreshold) {
      return false;
    }
    if (coordinator.hasMaxInFlightWriters()) {
      return true;
    }
    sendWritesToCoordinator();
    pendingWrites = new HashMap<>();
    return false;
  }

  private void sendWritesToCoordinator() {
    coordinator.addWrites(Collections.unmodifiableMap(pendingWrites));
  }

  // for the future it would be nice if we could reject writes and let the application decide how to proceed
  private void waitForWritesToUnblock() throws IOException {
    LOGGER.warning("There are too many in-flight write jobs. Waiting for them to finish");
    try {
      while (coordinator.hasMaxInFlightWriters()) {
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedExceptionWrapper("interrupted waiting for write jobs to finish");
    }
    lock.runWithWriteLock(this::sendWritesToCoordinator);
  }
}

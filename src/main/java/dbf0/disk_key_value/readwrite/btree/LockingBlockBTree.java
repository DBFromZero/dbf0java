package dbf0.disk_key_value.readwrite.btree;

import dbf0.common.Dbf0Util;
import dbf0.common.ReadTwoStepWriteLock;
import dbf0.disk_key_value.readwrite.CloseableReadWriteStorage;
import dbf0.disk_key_value.readwrite.blocks.VacuumChecker;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LockingBlockBTree<K extends Comparable<K>, V> implements BTree<K, V>, CloseableReadWriteStorage<K, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(LockingBlockBTree.class);

  private final BlockBTree<K, V> tree;
  private final BlockBTreeStorage<K, V> storage;
  private final VacuumChecker vacuumChecker;

  private final ReadTwoStepWriteLock lock;
  private transient Thread vacuumThread;

  public LockingBlockBTree(BlockBTree<K, V> tree, BlockBTreeStorage<K, V> storage, VacuumChecker vacuumChecker,
                           ReadTwoStepWriteLock lock) {
    this.tree = tree;
    this.storage = storage;
    this.vacuumChecker = vacuumChecker;
    this.lock = lock;
  }

  public LockingBlockBTree(BlockBTree<K, V> tree, BlockBTreeStorage<K, V> storage, VacuumChecker vacuumChecker) {
    this(tree, storage, vacuumChecker, new ReadTwoStepWriteLock());
  }

  @Override public void close() throws IOException {
    storage.close();
    if (vacuumThread != null) {
      try {
        vacuumThread.interrupt();
        vacuumThread.join();
        vacuumThread = null;
      } catch (InterruptedException e) {
        throw new RuntimeException("interrupted in closing vacuum thread");
      }
    }
  }

  public void initialize() throws IOException {
    tree.initialize();
  }

  @Override public long size() throws IOException {
    return lock.callWithReadLock(tree::size);
  }

  @Override public BTreeStorage<K, V> getStorage() {
    return storage;
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    lock.runWithWriteLocks(() -> tree.put(key, value));
    vacuumCheck();
  }

  @Nullable @Override public V get(@NotNull K key) throws IOException {
    return lock.callWithReadLock(() -> tree.get(key));
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    var deleted = lock.callWithWriteLocks(() -> tree.delete(key));
    if (deleted) {
      vacuumCheck();
    }
    return deleted;
  }

  @Override public Stream<Long> streamIdsInUse() throws IOException {
    return lock.callWithReadLock(() -> tree.streamIdsInUse().collect(Collectors.toList()).stream());
  }

  private void vacuumCheck() throws IOException {
    if (vacuumThread == null && lock.callWithReadLock(vacuumChecker::vacuumNeeded)) {
      LOGGER.fine("Starting vacuum thread");
      lock.runWithWriteLocks(() -> {
        if (vacuumThread == null) {
          vacuumThread = new Thread(new VacuumRunnable(storage.vacuum()));
          vacuumThread.start();
        }
      });
    }
  }

  private class VacuumRunnable implements Runnable {

    private final BlockBTreeStorage<K, V>.Vacuum vacuum;

    public VacuumRunnable(BlockBTreeStorage<K, V>.Vacuum vacuum) {
      this.vacuum = vacuum;
    }

    @Override public void run() {
      try {
        lock.runWithTwoStepWriteLocks(vacuum::writeNewFile, vacuum::commit);
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, e, () -> "error in vacuuming. aborting");
        vacuum.abort();
      } finally {
        vacuumThread = null;
      }
    }
  }
}

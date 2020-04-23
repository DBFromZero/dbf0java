package dbf0.disk_key_value.readwrite.btree;

import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.readwrite.blocks.VacuumChecker;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LockingBlockBTree<K extends Comparable<K>, V> implements BTree<K, V>, Closeable {

  private static final Logger LOGGER = Dbf0Util.getLogger(LockingBlockBTree.class);

  private final BlockBTree<K, V> tree;
  private final BlockBTreeStorage<K, V> storage;
  private final VacuumChecker vacuumChecker;

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private final Lock extraWriteLock = new ReentrantLock();
  private transient Thread vacuumThread;

  public LockingBlockBTree(BlockBTree<K, V> tree, BlockBTreeStorage<K, V> storage, VacuumChecker vacuumChecker) {
    this.tree = tree;
    this.storage = storage;
    this.vacuumChecker = vacuumChecker;
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

  void initialize() throws IOException {
    tree.initialize();
  }

  @Override public int size() throws IOException {
    return withReadLock(tree::size);
  }

  @Override public BTreeStorage<K, V> getStorage() {
    return storage;
  }

  @Override public void put(@NotNull K key, @NotNull V value) throws IOException {
    withWriteLocks(() -> {
      tree.put(key, value);
      return null;
    });
  }

  @Nullable @Override public V get(@NotNull K key) throws IOException {
    return withReadLock(() -> tree.get(key));
  }

  @Override public boolean delete(@NotNull K key) throws IOException {
    return withWriteLocks(() -> tree.delete(key));
  }

  @Override public Stream<Long> streamIdsInUse() throws IOException {
    return withReadLock(() -> tree.streamIdsInUse().collect(Collectors.toList()).stream());
  }

  interface Operation<T> {
    T run() throws IOException;
  }

  <T> T withReadLock(Operation<T> operation) throws IOException {
    readWriteLock.readLock().lock();
    try {
      return operation.run();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  <T> T withWriteLocks(Operation<T> operation) throws IOException {
    extraWriteLock.lock();
    try {
      T result;
      readWriteLock.writeLock().lock();
      try {
        result = operation.run();
      } finally {
        readWriteLock.writeLock().unlock();
      }
      if (vacuumThread == null && vacuumChecker.vacuumNeeded()) {
        LOGGER.fine("Starting vacuum thread");
        vacuumThread = new Thread(new VacuumRunnable(storage.vacuum()));
        vacuumThread.start();
      }
      return result;
    } finally {
      extraWriteLock.unlock();
    }
  }

  private class VacuumRunnable implements Runnable {

    private final BlockBTreeStorage<K, V>.Vacuum vacuum;

    public VacuumRunnable(BlockBTreeStorage<K, V>.Vacuum vacuum) {
      this.vacuum = vacuum;
    }

    @Override public void run() {
      extraWriteLock.lock();
      try {
        vacuum.writeNewFile();
        readWriteLock.writeLock().lock();
        try {
          vacuum.commit();
        } finally {
          readWriteLock.writeLock().unlock();
        }
      } catch (IOException e) {
        LOGGER.log(Level.SEVERE, e, () -> "error in vacuuming. aborting");
        vacuum.abort();
      } finally {
        extraWriteLock.unlock();
        vacuumThread = null;
      }
    }
  }
}

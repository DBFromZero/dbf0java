package dbf0.common;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockHelper {

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  public interface IOCallable<T> {
    T call() throws IOException;
  }

  public interface IORunnable {
    void run() throws IOException;
  }

  public <T> T callWithReadLock(IOCallable<T> callable) throws IOException {
    readWriteLock.readLock().lock();
    try {
      return callable.call();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public <T> T callWithReadLockUnchecked(IOCallable<T> callable) {
    readWriteLock.readLock().lock();
    try {
      return callable.call();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void runWithReadLock(IORunnable runnable) throws IOException {
    readWriteLock.readLock().lock();
    try {
      runnable.run();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public <T> T callWithWriteLock(IOCallable<T> callable) throws IOException {
    readWriteLock.writeLock().lock();
    try {
      return callable.call();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void runWithWriteLock(IORunnable runnable) throws IOException {
    readWriteLock.writeLock().lock();
    try {
      runnable.run();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void runWithWriteLockUnchecked(IORunnable runnable) {
    readWriteLock.writeLock().lock();
    try {
      runnable.run();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }
}

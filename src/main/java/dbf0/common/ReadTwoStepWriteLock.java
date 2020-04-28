package dbf0.common;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Utility for extending Read/Write lock functionality for the case
 * where some writes can be broken up into two parts.
 * A) An initial expensive step where reads can still be performed, but other writes must wait.
 * B) A second step to commit the results of the first step, during which reads cannot be allowed either.
 * <p>
 * Useful for databases that perform vacuuming or base merging, in that the initial step involve a lot
 * of time consuming, expensive IO, and the second step can quickly update internal structures to point
 * to the newly constructed results.
 */
public class ReadTwoStepWriteLock {

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private final Lock extraWriteLock = new ReentrantLock();

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

  public void runWithReadLock(IORunnable runnable) throws IOException {
    readWriteLock.readLock().lock();
    try {
      runnable.run();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public <T> T callWithWriteLocks(IOCallable<T> callable) throws IOException {
    extraWriteLock.lock();
    try {
      readWriteLock.writeLock().lock();
      try {
        return callable.call();
      } finally {
        readWriteLock.writeLock().unlock();
      }
    } finally {
      extraWriteLock.unlock();
    }
  }

  public void runWithWriteLocks(IORunnable runnable) throws IOException {
    extraWriteLock.lock();
    try {
      readWriteLock.writeLock().lock();
      try {
        runnable.run();
      } finally {
        readWriteLock.writeLock().unlock();
      }
    } finally {
      extraWriteLock.unlock();
    }
  }

  public <A, B> Pair<A, B> callWithTwoStepWriteLocks(IOCallable<A> callableWhileAllowingReads,
                                                     IOCallable<B> callableWithoutAllowingReads) throws IOException {
    extraWriteLock.lock();
    try {
      A a = callableWhileAllowingReads.call();
      readWriteLock.writeLock().lock();
      try {
        B b = callableWithoutAllowingReads.call();
        return Pair.of(a, b);
      } finally {
        readWriteLock.writeLock().unlock();
      }
    } finally {
      extraWriteLock.unlock();
    }
  }

  public void runWithTwoStepWriteLocks(IORunnable runnableCallableWhileAllowingReads,
                                       IORunnable runnableCallableWithoutAllowingReads) throws IOException {
    extraWriteLock.lock();
    try {
      runnableCallableWhileAllowingReads.run();
      readWriteLock.writeLock().lock();
      try {
        runnableCallableWithoutAllowingReads.run();
      } finally {
        readWriteLock.writeLock().unlock();
      }
    } finally {
      extraWriteLock.unlock();
    }
  }
}

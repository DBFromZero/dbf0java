package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readonly.KeyValueFileIterator;
import dbf0.disk_key_value.readonly.KeyValueFileReader;
import dbf0.disk_key_value.readonly.RandomAccessKeyValueFileReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class BaseDeltaFiles<T extends OutputStream> {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseDeltaFiles.class);
  private static final int BUFFER_SIZE = 0x4000;
  public static final String DELTA_PREFIX = "delta-";

  private final FileDirectoryOperations<T> directoryOperations;
  private final FileOperations<T> baseOperations;
  private final FileOperations<T> baseIndexOperations;
  private final TreeMap<Integer, ReaderWrapper> orderedInUseDeltas = new TreeMap<>();
  private ReaderWrapper baseWrapper;
  private int nextDelta = 0;

  public BaseDeltaFiles(FileDirectoryOperations<T> directoryOperations) {
    this.directoryOperations = directoryOperations;
    this.baseOperations = directoryOperations.file("base");
    this.baseIndexOperations = directoryOperations.file("base-index");
  }

  public boolean baseFileExists() {
    return baseOperations.exists();
  }

  public boolean hasInUseBase() {
    return baseWrapper != null;
  }

  public void loadExisting() throws IOException {
    if (!directoryOperations.exists()) {
      directoryOperations.mkdirs();
      return;
    }
    if (baseOperations.exists()) {
      Preconditions.checkState(baseIndexOperations.exists());
      setBase();
    }
    int maxDelta = -1;
    var deltaPattern = Pattern.compile(DELTA_PREFIX + "(\\d+)$");
    for (var path : directoryOperations.list()) {
      var matcher = deltaPattern.matcher(path);
      if (matcher.matches()) {
        var delta = Integer.parseInt(matcher.group(1));
        addDelta(delta);
        maxDelta = Math.max(maxDelta, delta);
      }
    }
    nextDelta = maxDelta + 1;
  }

  // ordered oldest to newest
  public synchronized List<Integer> getOrderedDeltasInUse() {
    return new ArrayList<>(orderedInUseDeltas.keySet());
  }

  @Nullable public ByteArrayWrapper searchForKey(ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(hasInUseBase());
    List<ReaderWrapper> orderedDeltas;
    synchronized (this) {
      orderedDeltas = new ArrayList<>(orderedInUseDeltas.values());
    }
    // prefer the later deltas and finally the base
    var deltaValue = recursivelySearchDeltasInReverse(orderedDeltas.iterator(), key);
    if (deltaValue != null) {
      return deltaValue;
    }
    return baseWrapper.lock.callWithReadLock(() -> Preconditions.checkNotNull(baseWrapper.reader).get(key));
  }

  @Nullable private ByteArrayWrapper recursivelySearchDeltasInReverse(
      Iterator<ReaderWrapper> iterator,
      ByteArrayWrapper key) throws IOException {
    if (!iterator.hasNext()) {
      return null;
    }
    var wrapper = iterator.next();
    var result = recursivelySearchDeltasInReverse(iterator, key);
    if (result != null) {
      return result;
    }
    // note that it is possible the delta was deleted while we were searching other deltas or
    // while we were waiting for the read lock.
    // that is fine because now the delta is in the base and we always search the deltas first
    // so if the key was in the delta then we'll find it when we search the base
    return wrapper.lock.callWithReadLock(() -> wrapper.reader == null ? null : wrapper.reader.get(key));
  }


  // these three methods should only be called by the delta writer coordinator
  void setBase() throws IOException {
    LOGGER.info("Setting initial base");
    Preconditions.checkState(!hasInUseBase());
    var reader = createRandomAccessReader(baseOperations, baseIndexOperations);
    synchronized (this) {
      baseWrapper = new ReaderWrapper(reader);
    }
  }

  synchronized int allocateDelta() {
    return nextDelta++;
  }

  synchronized void addDelta(Integer delta) throws IOException {
    LOGGER.info("Adding delta " + delta);
    Preconditions.checkState(hasInUseBase());
    orderedInUseDeltas.put(delta, new ReaderWrapper(createRandomAccessReader(getDeltaOperations(delta), getDeltaIndexOperations(delta))));
  }

  // these three methods should only be called by the merger cron
  RandomAccessKeyValueFileReader getBaseReaderForMerge() {
    Preconditions.checkState(hasInUseBase());
    return baseWrapper.reader;
  }

  void commitNewBase(FileOperations.OverWriter<T> baseOverWriter,
                     FileOperations.OverWriter<T> baseIndexOverWriter) throws IOException {
    LOGGER.info("Committing new base");
    Preconditions.checkState(hasInUseBase());
    // this operation can take a non-trivial amount of time since we have to read the index
    // ideally we could load the index from temp file or key it in memory while writing, but
    // I don't think that is a worthwhile optimization, although it will cause all other base
    // operations to block
    baseWrapper.lock.runWithWriteLock(() -> {
      baseOverWriter.commit();
      baseIndexOverWriter.commit();
      baseWrapper.reader = createRandomAccessReader(baseOperations, baseIndexOperations);
    });
  }

  public void deleteDelta(Integer delta) throws IOException {
    LOGGER.info("Deleting delta " + delta);
    ReaderWrapper wrapper;
    synchronized (this) {
      wrapper = orderedInUseDeltas.remove(delta);
    }
    Preconditions.checkArgument(wrapper != null, "No such delta " + delta + " in use");
    wrapper.lock.runWithWriteLock(() -> {
      wrapper.reader = null; // mark the reader as deleted incase a read lock is acquired after this
      getDeltaOperations(delta).delete();
      getDeltaIndexOperations(delta).delete();
    });
  }

  FileOperations<T> getBaseOperations() {
    return baseOperations;
  }

  FileOperations<T> getBaseIndexOperations() {
    return baseIndexOperations;
  }

  FileOperations<T> getDeltaOperations(Integer delta) {
    return directoryOperations.file(DELTA_PREFIX + delta);
  }

  FileOperations<T> getDeltaIndexOperations(Integer delta) {
    Preconditions.checkArgument(delta >= 0);
    return directoryOperations.file(DELTA_PREFIX + delta + "-index");
  }

  @NotNull private KeyValueFileReader createReader(FileOperations<T> operations) throws IOException {
    return new KeyValueFileReader(new BufferedInputStream(operations.createInputStream(), BUFFER_SIZE));
  }

  @NotNull private RandomAccessKeyValueFileReader createRandomAccessReader(FileOperations<T> baseOperations,
                                                                           FileOperations<T> indexOperations) throws IOException {
    return new RandomAccessKeyValueFileReader(baseOperations, RandomAccessKeyValueFileReader.readIndex(
        new KeyValueFileIterator(createReader(indexOperations))));
  }

  private static class ReaderWrapper {
    @Nullable private RandomAccessKeyValueFileReader reader;
    private final ReadWriteLockHelper lock = new ReadWriteLockHelper();

    private ReaderWrapper(@NotNull RandomAccessKeyValueFileReader reader) {
      this.reader = Preconditions.checkNotNull(reader);
    }
  }
}

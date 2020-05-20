package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.ReadWriteLockHelper;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readonly.base.BaseRandomAccessKeyValueFileReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class BaseDeltaFiles<T extends OutputStream, K, V, R extends BaseRandomAccessKeyValueFileReader<K, V, ?>> {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseDeltaFiles.class);
  public static final String DELTA_PREFIX = "delta-";

  private final FileDirectoryOperations<T> directoryOperations;
  private final RandomAccessOpener<T, K, V, R> opener;

  private final FileOperations<T> baseOperations;
  private final FileOperations<T> baseIndexOperations;
  private final TreeMap<Integer, ReaderWrapper<K, V, R>> orderedInUseDeltas = new TreeMap<>();
  private ReaderWrapper<K, V, R> baseWrapper;
  private int nextDelta = 0;

  public BaseDeltaFiles(FileDirectoryOperations<T> directoryOperations,
                        RandomAccessOpener<T, K, V, R> opener) {
    this.directoryOperations = Preconditions.checkNotNull(directoryOperations);
    this.opener = Preconditions.checkNotNull(opener);
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
  @NotNull public synchronized List<Integer> getOrderedDeltasInUse() {
    return new ArrayList<>(orderedInUseDeltas.keySet());
  }

  @NotNull public List<ReaderWrapper<K, V, R>> getOrderedDeltaReaders() {
    synchronized (this) {
      return new ArrayList<>(orderedInUseDeltas.values());
    }
  }

  // these three methods should only be called by the delta writer coordinator
  public void setBase() throws IOException {
    LOGGER.info("Setting initial base");
    Preconditions.checkState(!hasInUseBase());
    var reader = opener.open(baseOperations, baseIndexOperations);
    synchronized (this) {
      baseWrapper = new ReaderWrapper<>(reader);
    }
  }

  public ReaderWrapper<K, V, R> getBaseWrapper() {
    return baseWrapper;
  }

  public synchronized int allocateDelta() {
    return nextDelta++;
  }

  public void addDelta(Integer delta) throws IOException {
    LOGGER.info("Adding delta " + delta);
    Preconditions.checkState(hasInUseBase());
    synchronized (this) {
      orderedInUseDeltas.put(delta, new ReaderWrapper<>(opener.open(getDeltaOperations(delta), getDeltaIndexOperations(delta))));
    }
  }

  public void commitNewBase(FileOperations.OverWriter<T> baseOverWriter,
                            FileOperations.OverWriter<T> baseIndexOverWriter) throws IOException {
    LOGGER.info("Committing new base");
    Preconditions.checkState(hasInUseBase());
    // this operation can take a non-trivial amount of time since we have to read the index
    // ideally we could load the index from temp file or key it in memory while writing, but
    // I don't think that is a worthwhile optimization, although it will cause all other base
    // operations to block
    baseWrapper.getLock().runWithWriteLock(() -> {
      baseOverWriter.commit();
      baseIndexOverWriter.commit();
      baseWrapper.setReader(opener.open(baseOperations, baseIndexOperations));
    });
  }

  public void deleteDelta(Integer delta) throws IOException {
    LOGGER.info("Deleting delta " + delta);
    ReaderWrapper<K, V, R> wrapper;
    synchronized (this) {
      wrapper = orderedInUseDeltas.remove(delta);
    }
    Preconditions.checkArgument(wrapper != null, "No such delta " + delta + " in use");
    wrapper.getLock().runWithWriteLock(() -> {
      wrapper.setReader(null); // mark the reader as deleted incase a read lock is acquired after this
      getDeltaOperations(delta).delete();
      getDeltaIndexOperations(delta).delete();
    });
  }

  public FileOperations<T> getBaseOperations() {
    return baseOperations;
  }

  public FileOperations<T> getBaseIndexOperations() {
    return baseIndexOperations;
  }

  public FileOperations<T> getDeltaOperations(Integer delta) {
    return directoryOperations.file(DELTA_PREFIX + delta);
  }

  public FileOperations<T> getDeltaIndexOperations(Integer delta) {
    Preconditions.checkArgument(delta >= 0);
    return directoryOperations.file(DELTA_PREFIX + delta + "-index");
  }

  public interface RandomAccessOpener<T extends OutputStream, K, V, R extends BaseRandomAccessKeyValueFileReader<K, V, ?>> {
    R open(FileOperations<T> baseOperations, FileOperations<T> indexOperations) throws IOException;
  }

  public static class ReaderWrapper<K, V, R extends BaseRandomAccessKeyValueFileReader<K, V, ?>> {
    @Nullable private R reader;
    private final ReadWriteLockHelper lock = new ReadWriteLockHelper();

    protected ReaderWrapper(@NotNull R reader) {
      this.reader = Preconditions.checkNotNull(reader);
    }

    public @Nullable R getReader() {
      return reader;
    }

    private void setReader(@Nullable R reader) {
      this.reader = reader;
    }

    public ReadWriteLockHelper getLock() {
      return lock;
    }
  }
}

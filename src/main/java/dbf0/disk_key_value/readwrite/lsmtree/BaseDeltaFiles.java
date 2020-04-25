package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.disk_key_value.FileDirectoryOperations;
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

public class BaseDeltaFiles<T extends OutputStream> {

  private static final int BUFFER_SIZE = 0x4000;

  private final FileDirectoryOperations<T> directoryOperations;
  private final FileOperations<T> baseOperations;
  private final FileOperations<T> baseIndexOperations;
  private final TreeMap<Integer, RandomAccessKeyValueFileReader> orderedInUseDeltas = new TreeMap<>();
  private RandomAccessKeyValueFileReader baseRandomReader;
  private int nextDelta = 0;

  public BaseDeltaFiles(FileDirectoryOperations<T> directoryOperations) {
    this.directoryOperations = directoryOperations;
    this.baseOperations = directoryOperations.file("base");
    this.baseIndexOperations = directoryOperations.file("base-index");
  }

  public boolean baseFileExists() {
    return baseOperations.exists();
  }

  public RandomAccessKeyValueFileReader baseRandomReader() {
    return baseRandomReader;
  }

  public boolean hasInUseBase() {
    return baseRandomReader != null;
  }

  public void resetBaseRandomReader() throws IOException {
    baseRandomReader = createRandomAccessReader(baseOperations, baseIndexOperations);
  }

  public int allocateDelta() {
    return nextDelta++;
  }

  public synchronized void addDelete(Integer delta) throws IOException {
    Preconditions.checkState(hasInUseBase());
    orderedInUseDeltas.put(delta, createRandomAccessReader(getDeltaOperations(delta), getDeltaIndexOperations(delta)));
  }

  public synchronized void removeDelta(Integer delta) {
    var result = orderedInUseDeltas.remove(delta);
    Preconditions.checkArgument(result != null, "No such delta " + delta + " in use");
  }

  @Nullable public ByteArrayWrapper searchForKey(ByteArrayWrapper key) throws IOException {
    Preconditions.checkState(hasInUseBase());
    List<RandomAccessKeyValueFileReader> orderedDeltaReaders;
    synchronized (this) {
      orderedDeltaReaders = new ArrayList<>(orderedInUseDeltas.values());
    }
    // prefer the latest deltas
    var deltaValue = recursivelySearchDeltasInReverse(orderedDeltaReaders.iterator(), key);
    if (deltaValue != null) {
      return deltaValue;
    }
    return baseRandomReader.get(key);
  }

  @Nullable private ByteArrayWrapper recursivelySearchDeltasInReverse(
      Iterator<RandomAccessKeyValueFileReader> iterator,
      ByteArrayWrapper key) throws IOException {
    if (!iterator.hasNext()) {
      return null;
    }
    var reader = iterator.next();
    var result = recursivelySearchDeltasInReverse(iterator, key);
    if (result != null) {
      return result;
    }
    return reader.get(key);
  }

  public FileOperations<T> getBaseOperations() {
    return baseOperations;
  }

  public FileOperations<T> getDeltaOperations(Integer delta) {
    return directoryOperations.file("delta" + delta);
  }

  public FileOperations<T> getBaseIndexOperations() {
    return baseIndexOperations;
  }

  public FileOperations<T> getDeltaIndexOperations(Integer delta) {
    Preconditions.checkArgument(delta >= 0);
    return directoryOperations.file("delta-" + delta + "-index");
  }

  public KeyValueFileReader createBaseReader() throws IOException {
    return createReader(baseOperations);
  }

  private KeyValueFileReader deltaReader(Integer delta) throws IOException {
    return createReader(getDeltaOperations(delta));
  }

  public KeyValueFileIterator baseIterator() throws IOException {
    return new KeyValueFileIterator(createBaseReader());
  }

  public KeyValueFileIterator deltaIterator(Integer delta) throws IOException {
    return new KeyValueFileIterator(deltaReader(delta));
  }

  @NotNull private KeyValueFileReader createReader(FileOperations<T> operations) throws IOException {
    return new KeyValueFileReader(new BufferedInputStream(operations.createInputStream(), BUFFER_SIZE));
  }

  @NotNull private RandomAccessKeyValueFileReader createRandomAccessReader(FileOperations<T> baseOperations,
                                                                           FileOperations<T> indexOperations) throws IOException {
    return new RandomAccessKeyValueFileReader(baseOperations, RandomAccessKeyValueFileReader.readIndex(
        new KeyValueFileIterator(createReader(indexOperations))));
  }
}

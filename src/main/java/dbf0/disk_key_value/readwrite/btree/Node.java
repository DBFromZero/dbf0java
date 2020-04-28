package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public abstract class Node<K extends Comparable<K>, V> {

  private static final Logger LOGGER = Dbf0Util.getLogger(Node.class);

  protected final K[] keys;
  protected int count;
  @NotNull protected final BTreeStorage<K, V> storage;
  final long id;
  private long parentId;

  protected Node(long id, int capacity, @NotNull BTreeStorage<K, V> storage) {
    this(id, BTreeStorage.NO_ID, 0, (K[]) new Comparable[capacity], storage);
  }

  protected Node(long id, long parentId, int count, @NotNull K[] keys, @NotNull BTreeStorage<K, V> storage) {
    Preconditions.checkArgument(keys.length > 0 && keys.length % 2 == 0);
    Preconditions.checkArgument(count >= 0);
    Preconditions.checkArgument(count <= keys.length);
    this.id = id;
    this.parentId = parentId;
    this.count = count;
    this.keys = keys;
    this.storage = storage;
  }

  int getCount() {
    return count;
  }

  int getCapacity() {
    return keys.length;
  }

  boolean isFull() {
    return getCapacity() == getCount();
  }

  abstract int size();

  abstract Node<K, V> put(K key, V value);

  @Nullable abstract V get(K key);

  abstract boolean delete(K key);

  abstract K maxKey();

  abstract K minKey();

  abstract void recursivelyPrint(int depth);

  protected abstract Node<K, V> performSplit(int start, int end);

  @Override public String toString() {
    return baseToStringHelper().toString();
  }

  @NotNull protected MoreObjects.ToStringHelper baseToStringHelper() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("keys",
            "[" + Joiner.on(",").join(Arrays.stream(keys).limit(count)
                .map(x -> x == null ? "null" : x)
                .collect(Collectors.toList())) + "]")
        .add("count", count)
        .add("parentId", getParentId());
  }

  List<K> getKeys() {
    return Arrays.asList(keys).subList(0, count);
  }

  long getId() {
    return id;
  }

  long getParentId() {
    return parentId;
  }

  protected void setParent(long parentId) {
    if (parentId != this.parentId) {
      this.parentId = parentId;
      nodeChanged();
    }
  }

  protected void setParent(ParentNode<K, V> parent) {
    setParent(parent.id);
  }

  protected void clearParent() {
    this.parentId = BTreeStorage.NO_ID;
    // don't store node since this generally happens as part of delete
  }

  protected void nodeChanged() {
    storage.nodeChanged(this);
  }

  protected Optional<ParentNode<K, V>> optionalParent() {
    return getParentId() == BTreeStorage.NO_ID ? Optional.empty() :
        Optional.of((ParentNode<K, V>) storage.getNode(getParentId()));
  }

  protected int binarySearch(K key) {
    Preconditions.checkState(count > 0);
    return Arrays.binarySearch(keys, 0, count, key);
  }

  protected ParentNode<K, V> split(K keyToAdd) {
    LOGGER.fine(() -> String.format("splitting %s to add %s", this, keyToAdd));
    Preconditions.checkState(isFull());
    var parentOptional = optionalParent();
    var useCurrentParent = parentOptional.map(x -> !x.isFull()).orElse(false);
    var splitParent = useCurrentParent ? parentOptional.get() : storage.createParent();
    parentOptional.ifPresent(parent -> parent.removeNode(this));
    int mid = count / 2;
    var split = keyToAdd.compareTo(keys[mid]) < 0 ? performSplit(0, mid) : performSplit(mid, count);
    nodeChanged();
    splitParent.addNode(split);
    splitParent.addNode(this);
    if (!useCurrentParent) {
      parentOptional.ifPresent(parent -> parent.addNode(splitParent));
    }
    return splitParent;
  }

  protected void splitHelper(int start, int end, Node<K, V> dest, Object[] srcValues, Object[] destValues) {
    var elementsToCopy = end - start;
    Preconditions.checkState(elementsToCopy > 0);
    System.arraycopy(srcValues, start, destValues, 0, elementsToCopy);
    System.arraycopy(keys, start, dest.keys, 0, elementsToCopy);
    dest.count = elementsToCopy;

    var mid = count / 2;
    Preconditions.checkState(count - mid == elementsToCopy);

    if (start == 0) {
      // we split out the beginning of this leaf, shift remaining elements to front
      Preconditions.checkState(mid == end);
      for (int i = 0; i < mid; i++) {
        keys[i] = keys[i + mid];
        srcValues[i] = srcValues[i + mid];
      }
    } else {
      // we split out the end, nothing to move
      Preconditions.checkState(start == mid && end == count);
    }
    // always null out the end to remove references
    for (int i = mid; i < count; i++) {
      keys[i] = null;
      srcValues[i] = null;
    }
    count -= elementsToCopy;
  }

  static int invertNegativeBinarySearchIndex(int index) {
    return -(index + 1);
  }

  static void arrayShiftUp(Object[] array, int index, int count) {
    Preconditions.checkState(index >= 0 && index + count < array.length && count >= 0);
    System.arraycopy(array, index, array, index + 1, count);
  }

  static void arrayShiftDown(Object[] array, int index, int count) {
    Preconditions.checkState(index >= 0);
    Preconditions.checkState(index + count < array.length);
    System.arraycopy(array, index + 1, array, index, count);
  }
}

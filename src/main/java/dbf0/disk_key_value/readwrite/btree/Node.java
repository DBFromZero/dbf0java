package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Arrays;

public abstract class Node<K extends Comparable<K>, V> {

  protected final K[] keys;
  protected int count = 0;
  protected ParentNode<K, V> parent = null;

  protected Node(int capacity) {
    Preconditions.checkArgument(capacity > 0 && capacity % 2 == 0);
    this.keys = (K[]) new Comparable[capacity];
  }

  @VisibleForTesting protected Node(int count, K @NotNull [] keys) {
    Preconditions.checkArgument(count >= 0);
    Preconditions.checkArgument(count <= keys.length);
    this.keys = keys;
    this.count = count;
  }

  @VisibleForTesting
  void setParent(ParentNode<K, V> parent) {
    this.parent = parent;
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

  protected int binarySearch(K key) {
    Preconditions.checkState(count > 0);
    return Arrays.binarySearch(keys, 0, count, key);
  }

  protected ParentNode<K, V> split(K keyToAdd) {
    Preconditions.checkState(isFull());
    var useCurrentParent = parent != null && !parent.isFull();
    var splitParent = useCurrentParent ? parent : new ParentNode<K, V>(getCapacity());
    if (parent != null) {
      parent.removeNode(this);
    }
    int mid = count / 2;
    var split = keyToAdd.compareTo(keys[mid]) < 0 ? performSplit(0, mid) : performSplit(mid, count);
    splitParent.addNode(split);
    splitParent.addNode(this);
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
    Preconditions.checkState(index > 0 && index >= count);
    System.arraycopy(array, index, array, index - 1, count);
  }
}

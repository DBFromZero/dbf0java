package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
  protected final V[] values;

  public LeafNode(int capacity) {
    super(capacity);
    this.values = (V[]) new Object[capacity];
  }

  LeafNode(int capacity, ParentNode<K, V> parent) {
    this(capacity);
    this.parent = parent;
  }

  @Override public int size() {
    return count;
  }

  @Nullable @Override public V get(K key) {
    if (count == 0) {
      return null;
    }
    var index = binarySearch(key);
    if (index < 0) {
      return null;
    }
    if (keys[index].equals(key)) {
      return values[index];
    }
    return null;
  }

  @Override public Node<K, V> put(K key, V value) {
    if (count == 0) {
      keys[0] = key;
      values[0] = value;
      count++;
      return this;
    }

    var index = binarySearch(key);
    if (index >= 0) {
      values[index] = value;
      return this;
    }

    if (isFull()) {
      return split(key).put(key, value);
    }
    int insertIndex = invertNegativeBinarySearchIndex(index);
    var n = count - insertIndex;
    if (n > 0) {
      arrayShiftUp(keys, insertIndex, n);
      arrayShiftUp(values, insertIndex, n);
    }
    keys[insertIndex] = key;
    values[insertIndex] = value;
    count++;
    return this;
  }

  @Override boolean delete(K key) {
    if (count == 0) {
      return false;
    }
    var index = binarySearch(key);
    if (index < 0) {
      return false;
    }
    int n = count - index - 1;
    var prevMaxKey = maxKey();
    if (n > 0) {
      arrayShiftDown(keys, index, n);
      arrayShiftDown(values, index, n);
    }
    count--;
    if (parent != null) {
      if (count == 0) {
        parent.deleteChild(this, prevMaxKey);
      } else {
        parent.handleChildDeleteKey(this, prevMaxKey, maxKey());
      }
    }
    return true;
  }

  @Override protected LeafNode<K, V> performSplit(int start, int end) {
    var newLeaf = new LeafNode<K, V>(getCapacity());
    splitHelper(start, end, newLeaf, values, newLeaf.values);
    return newLeaf;
  }

  @Override public K maxKey() {
    Preconditions.checkState(count > 0);
    return keys[count - 1];
  }

  @Override public K minKey() {
    Preconditions.checkState(count > 0);
    return keys[0];
  }

  @Override public void recursivelyPrint(int depth) {
    var prefix = " -".repeat(depth);
    System.out.println(prefix + getClass().getSimpleName() + " sz=" + size());
    for (int i = 0; i < count; i++) {
      System.out.println(prefix + "  " + keys[i] + "=" + values[i]);
    }
  }

  /**
   * Destructively modifies the first leaf node.
   * Assumes nodes are ordered by key.
   */
  static <K extends Comparable<K>, V> void combine(Node<K, V>[] nodes, int start, int count) {
    var compacted = (LeafNode<K, V>) nodes[start];
    for (int i = start + 1, end = start + count; i < end; i++) {
      var src = (LeafNode<K, V>) nodes[i];
      System.arraycopy(src.values, 0, compacted.values, compacted.count, src.count);
      System.arraycopy(src.keys, 0, compacted.keys, compacted.count, src.count);
      compacted.count += src.count;
    }
  }

  @Override public String toString() {
    return baseToStringHelper()
        .add("values", values)
        .toString();
  }
}

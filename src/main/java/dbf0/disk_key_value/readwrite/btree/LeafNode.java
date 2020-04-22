package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
  private final V[] values;

  LeafNode(long id, int capacity, @NotNull BTreeStorage<K, V> storage) {
    super(id, capacity, storage);
    this.values = (V[]) new Object[capacity];
  }

  LeafNode(long id, long parentId, int count, @NotNull K[] keys, V[] values, @NotNull BTreeStorage<K, V> storage) {
    super(id, parentId, count, keys, storage);
    Preconditions.checkArgument(keys.length == values.length);
    this.values = values;
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
    return values[index];
  }

  @Override public Node<K, V> put(K key, V value) {
    if (count == 0) {
      keys[0] = key;
      values[0] = value;
      count++;
    } else {
      var index = binarySearch(key);
      if (index >= 0) {
        values[index] = value;
      } else if (isFull()) {
        return split(key).put(key, value);
      } else {
        index = invertNegativeBinarySearchIndex(index);
        var n = count - index;
        if (n > 0) {
          arrayShiftUp(keys, index, n);
          arrayShiftUp(values, index, n);
        }
        keys[index] = key;
        values[index] = value;
        count++;
      }
    }
    nodeChanged();
    return this;
  }

  @Override boolean delete(K key) {
    //System.out.println("delete key=" + key + " in " + this);
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
    if (count == 0) {
      storage.deleteNode(id);
    } else {
      nodeChanged();
    }
    optionalParent().ifPresent(parent -> {
      if (count == 0) {
        parent.deleteChild(this, prevMaxKey);
      } else {
        parent.handleChildDeleteKey(this, prevMaxKey, maxKey());
      }
    });
    return true;
  }

  @Override protected LeafNode<K, V> performSplit(int start, int end) {
    var newLeaf = storage.createLeaf();
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

  @Override public String toString() {
    return baseToStringHelper()
        .add("values", values == null ? "<null>" :
            "[" + Joiner.on(",").join(Arrays.stream(values).limit(count).iterator()) + "]")
        .toString();
  }

  List<V> getValues() {
    return Arrays.asList(values).subList(0, count);
  }

  /**
   * Destructively modifies the first leaf node.
   * Assumes nodes are ordered by key.
   */
  static <K extends Comparable<K>, V> void combine(LeafNode<K, V> combined, Stream<LeafNode<K, V>> stream) {
    stream.forEach(src -> {
      System.arraycopy(src.values, 0, combined.values, combined.count, src.count);
      System.arraycopy(src.keys, 0, combined.keys, combined.count, src.count);
      combined.count += src.count;
      src.storage.deleteNode(src.id);
    });
    combined.nodeChanged();
  }
}

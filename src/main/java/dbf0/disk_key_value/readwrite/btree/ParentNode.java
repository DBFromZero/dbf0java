package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Arrays;

class ParentNode<K extends Comparable<K>, V> extends Node<K, V> {
  protected final Node<K, V>[] children;

  ParentNode(int capacity) {
    super(capacity);
    this.children = (Node<K, V>[]) new Node[capacity];
  }

  @VisibleForTesting ParentNode(int count, @NotNull K[] keys, Node<K, V> @NotNull [] children) {
    super(count, keys);
    Preconditions.checkState(keys.length == children.length);
    this.children = children;
  }

  @Override public int size() {
    return Arrays.stream(children).limit(count).mapToInt(Node::size).sum();
  }

  @Override public K maxKey() {
    Preconditions.checkState(count > 0);
    return keys[count - 1];
  }

  @Override public K minKey() {
    Preconditions.checkState(count > 0);
    return children[0].minKey();
  }

  @Nullable @Override public V get(K key) {
    Preconditions.checkState(count > 0);
    var index = binarySearch(key);
    if (index < 0) {
      index = invertNegativeBinarySearchIndex(index);
      if (index == count) {
        return null;
      }
    }
    return children[index].get(key);
  }

  @Override public Node<K, V> put(K key, V value) {
    Preconditions.checkState(count > 0);

    var index = binarySearch(key);
    if (index >= 0) {
      var child = children[index];
      var result = child.put(key, value);
      Preconditions.checkState(child == result, "should not split for existing key");
      return this;
    }

    index = invertNegativeBinarySearchIndex(index);
    if (index == count) {
      return putNewMaxKey(key, value);
    }

    var child = children[index];
    Preconditions.checkState(child != this);
    Preconditions.checkState(child.parent == this);

    // if key is in the range of this child, then we have to give it to that child
    if (child.minKey().compareTo(key) <= 0 && child.maxKey().compareTo(key) >= 0) {
      var result = child.put(key, value);
      // put can cause the child to split and it may reuse it's current parent node, which is `this`
      if (result != this && result != child) {
        Preconditions.checkState(result.parent == null);
        result.parent = this;
        children[index] = result;
      }
      return this;
    }

    // if the child is not full, prefer to add entry to leaf child rather than creating new node
    if (child instanceof LeafNode && !child.isFull()) {
      Preconditions.checkState(key.compareTo(child.minKey()) < 0);
      var result = child.put(key, value);
      Preconditions.checkState(result == child);
      return this;
    }

    if (isFull()) {
      return split(key).put(key, value);
    }

    insertNode(new LeafNode<K, V>(getCapacity(), this).put(key, value), index);
    return this;
  }

  private Node<K, V> putNewMaxKey(K key, V value) {
    var child = children[count - 1];
    if (!child.isFull()) {
      var result = child.put(key, value);
      Preconditions.checkState(result == child);
      keys[count - 1] = key;
      return this;
    }

    if (isFull()) {
      return split(key).put(key, value);
    }
    children[count] = new LeafNode<K, V>(getCapacity(), this).put(key, value);
    keys[count] = key;
    count++;
    return this;
  }

  @Override boolean delete(K key) {
    Preconditions.checkState(count > 0);
    var index = binarySearch(key);
    return index > 0 && children[index].delete(key);
  }

  @Override protected ParentNode<K, V> performSplit(int start, int end) {
    var newParent = new ParentNode<K, V>(getCapacity());
    for (int i = start; i < end; i++) {
      Preconditions.checkState(children[i].parent == this);
      children[i].parent = newParent;
    }
    splitHelper(start, end, newParent, children, newParent.children);
    return newParent;
  }

  void updateChildMaxKey(Node<K, V> child, K oldMaxKey, K newMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    keys[index] = newMaxKey;
    if (index == count - 1 && parent != null) {
      parent.updateChildMaxKey(this, oldMaxKey, newMaxKey);
    }
  }

  void handleChildDeleteKey(Node<K, V> child, K oldMaxKey, K newMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    if (!newMaxKey.equals(oldMaxKey)) {
      keys[index] = newMaxKey;
      if (index == count - 1 && parent != null) {
        parent.handleChildDeleteKey(this, oldMaxKey, newMaxKey);
      }
    }
    if (child instanceof LeafNode) {
      checkCombineAdjacentLeaves(index);
    }
  }

  void deleteChild(Node<K, V> child, K oldMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    arrayShiftDown(keys, index, 1);
    arrayShiftDown(children, index, 1);
    count--;
    if (parent != null) {
      if (count == 0) {
        parent.deleteChild(this, oldMaxKey);
      } else if (index == count) {
        parent.handleChildDeleteKey(this, oldMaxKey, maxKey());
      }
    }
  }

  private int getChildIndex(Node<K, V> child, K key) {
    Preconditions.checkState(child.parent == this);
    var index = binarySearch(key);
    Preconditions.checkState(index > 0);
    Preconditions.checkState(children[index] == child);
    return index;
  }

  void addNode(Node<K, V> node) {
    Preconditions.checkState(node.parent == null);
    node.parent = this;
    var maxKey = node.maxKey();
    if (count == 0) {
      keys[0] = maxKey;
      children[0] = node;
      count++;
      return;
    }
    var index = binarySearch(maxKey);
    Preconditions.checkState(index < 0);
    insertNode(node, -(index + 1));
  }

  private void insertNode(Node<K, V> node, int index) {
    Preconditions.checkState(node.parent == this);
    var n = count - index;
    if (n > 0) {
      arrayShiftUp(keys, index, n);
      arrayShiftUp(children, index, n);
    }
    keys[index] = node.maxKey();
    children[index] = node;
    count++;
  }

  void removeNode(Node<K, V> node) {
    Preconditions.checkState(node.parent == this);
    var index = binarySearch(node.maxKey());
    if (index < 0) {
      System.out.println("debug");
    }
    Preconditions.checkState(index >= 0);
    Preconditions.checkState(children[index] == node);

    for (int i = index; i < count - 1; i++) {
      keys[i] = keys[i + 1];
      children[i] = children[i + 1];
    }
    count--;
    keys[count] = null;
    children[count] = null;
    node.parent = null;
  }

  void checkCombineAdjacentLeaves(int index) {
    int start = searchAdjacentLeavesToCombine(index, 0);
    int end = searchAdjacentLeavesToCombine(index, count - 1);
    if (start == end) {
      return;
    }
    int bestStart = -1;
    int bestEnd = -1;
    int bestCombinedSize = 0;
    for (int i = start; i < end; i++) {
      for (int j = i + 1; j <= end; j++) {
        int combinedSize = 0;
        for (int k = i; k <= j; k++) {
          combinedSize += children[k].size();
        }
        if (combinedSize <= getCapacity() && combinedSize > bestCombinedSize) {
          bestCombinedSize = combinedSize;
          bestStart = i;
          bestEnd = j;
        }
      }
    }
    if (bestCombinedSize != 0) {
      combineAdjacentLeaves(0, bestStart, bestEnd - bestStart + 1);
    }
  }

  int searchAdjacentLeavesToCombine(int start, int inclusiveEnd) {
    int offset = start < inclusiveEnd ? 1 : -1;
    for (int i = start + offset; i != inclusiveEnd + offset; i += offset) {
      if (!(children[i] instanceof LeafNode)) {
        return i - offset;
      }
    }
    return inclusiveEnd;
  }

  void findAdjacentLeavesToCompact() {
    findAdjacentLeavesToCompact(0, 0);
  }

  private static void print(int depth, Object... args) {
    System.out.println(" -".repeat(depth) + Joiner.on(" ").join(args));
  }

  private void findAdjacentLeavesToCompact(int start, int depth) {
    print(depth, "findAdjacentLeavesToCompact ", hashCode(), start, count);
    int sumLeafSize = 0;
    for (int i = start; i < count; i++) {
      var child = children[i];
      print(depth, " X", hashCode(), "i=" + i, "c=" + count, "sumLSz=" + sumLeafSize,
          child.getClass().getSimpleName() + child.hashCode(), "cc=" + child.getCount(), "cz=" + child.size());
      int n = i - start;
      if (child instanceof LeafNode) {
        int newSumLeafSize = sumLeafSize + child.size();
        if (newSumLeafSize > child.getCapacity()) {
          print(depth, "over limit", newSumLeafSize);
          combineAdjacentLeaves(depth, start, n);
          findAdjacentLeavesToCompact(start + 1, depth);
          return;
        } else {
          sumLeafSize = newSumLeafSize;
        }
      } else {
        combineAdjacentLeaves(depth, start, n);
        ((ParentNode) child).findAdjacentLeavesToCompact(0, depth + 1);
        findAdjacentLeavesToCompact(i + 1 - Math.max(0, n - 1), depth);
        return;
      }
    }
    combineAdjacentLeaves(depth, start, count - start);
  }

  private void combineAdjacentLeaves(int depth, int start, int n) {
    if (n <= 1) {
      return;
    }
    print(depth, "performLeafCompaction", hashCode(), start, n, count);
    var initialSize = size();

    LeafNode.combine(children, start, n);
    keys[start] = children[start].maxKey();
    print(depth, "combined size", children[start].size());

    int removed = n - 1;
    int endShift = count - removed;
    print(depth, "shift range", start + 1, endShift);
    for (int i = start + 1; i < endShift; i++) {
      int j = i + removed;
      print(depth, "shifting", j, "to", i);
      Preconditions.checkState(j < count);
      Preconditions.checkState(keys[j] != null);
      Preconditions.checkState(children[j] != null);
      keys[i] = keys[j];
      children[i] = children[j];
    }
    for (int i = count - removed; i < count; i++) {
      keys[i] = null;
      children[i] = null;
    }
    count -= removed;
    Preconditions.checkState(count > 0);
    Preconditions.checkState(keys[count - 1] != null);

    Preconditions.checkState(size() == initialSize, "%s!=%s", initialSize, size());
  }

  @Override public void recursivelyPrint(int depth) {
    var prefix = " -".repeat(depth);
    System.out.println(prefix + getClass().getSimpleName() + " sz=" + size() +
        " count=" + getCount() +
        " from " + minKey() + " to " + maxKey());
    for (int i = 0; i < count; i++) {
      children[i].recursivelyPrint(depth + 1);
      System.out.println(prefix + keys[i]);
    }
  }
}

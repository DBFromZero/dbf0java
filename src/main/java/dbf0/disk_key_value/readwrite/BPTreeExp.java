package dbf0.disk_key_value.readwrite;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Experimentation with B+Trees
 */
public class BPTreeExp {


  public static void main(String[] args) throws Exception {
    var tree = new BTree<Integer, Integer>(16);
    var random = new Random(0xCAFE);
    IntStream.range(0, 1000).forEach(i -> tree.put(random.nextInt(100000), i));

    System.out.println();
    tree.root.recursivelyPrint(0);

    //System.out.println();
    //tree.compact();

    //System.out.println();
    //tree.root.recursivelyPrint(0);

    System.out.println();
    Arrays.stream(((ParentNode) tree.root).children)
        .filter(Objects::nonNull)
        .forEach(n -> System.out.println(n.getClass().getSimpleName() + " " + n.size() + " " + n.minKey() + " " + n.maxKey()));

    /*
    var ints = new int[]{2, 4, 6};
    IntStream.range(0, 10).forEach(i ->
        System.out.println(i + " " + Arrays.binarySearch(ints, i)));
  */
    /*
    var arr = IntStream.range(0, 6).boxed().toArray(Integer[]::new);
    arrayShift(arr, 0, 5);

    System.out.println(Arrays.asList(arr));
  */
  }

  private static class BTree<K extends Comparable<K>, V> {
    private Node<K, V> root;

    private BTree(int capacity) {
      root = new LeafNode<K, V>(capacity);
    }

    int size() {
      return root.size();
    }

    void put(K key, V value) {
      root = root.put(key, value);
    }

    @Nullable V get(K key) {
      return root.get(key);
    }

    void compact() {
      if (root instanceof ParentNode) {
        ((ParentNode<K, V>) root).findAdjacentLeavesToCompact();
      }
    }
  }

  private static abstract class Node<K extends Comparable<K>, V> {
    protected final K[] keys;
    protected int count = 0;
    protected ParentNode<K, V> parent = null;

    protected Node(int capacity) {
      Preconditions.checkArgument(capacity > 0 && capacity % 2 == 0);
      this.keys = (K[]) new Comparable[capacity];
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
  }

  private static class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
    protected final V[] values;

    LeafNode(int capacity) {
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
      var insertIndex = -(index + 1);
      var n = count - insertIndex;
      if (n > 0) {
        arrayShift(keys, insertIndex, n);
        arrayShift(values, insertIndex, n);
      } else if (parent != null) {
        parent.updateChildMaxKey(this, keys[count - 1], key);
      }

      keys[insertIndex] = key;
      values[insertIndex] = value;
      count++;
      return this;
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
    static <K extends Comparable<K>, V> void compact(Node<K, V>[] nodes, int start, int count) {
      var compacted = (LeafNode<K, V>) nodes[start];
      for (int i = start + 1, end = start + count; i < end; i++) {
        var src = (LeafNode<K, V>) nodes[i];
        System.arraycopy(src.values, 0, compacted.values, compacted.count, src.count);
        System.arraycopy(src.keys, 0, compacted.keys, compacted.count, src.count);
        compacted.count += src.count;
      }
    }
  }

  private static class ParentNode<K extends Comparable<K>, V> extends Node<K, V> {
    protected final Node<K, V>[] children;

    protected ParentNode(int capacity) {
      super(capacity);
      Preconditions.checkArgument(capacity > 2);
      Preconditions.checkArgument(capacity % 2 == 0);
      this.children = (Node<K, V>[]) new Node[capacity];
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
        index = -(index + 1);
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

      index = -(index + 1);
      if (index == count) {
        var child = children[index - 1];
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

      var child = children[index];
      Preconditions.checkState(child != this);
      if (child.parent != this) {
        System.out.println("debug");
      }
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

      // if the child is not full, prefer add node to leaf child rather than creating new node
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

    void updateChildMaxKey(Node<K, V> child, K oldMaxKey, K newMaxKey) {
      var index = binarySearch(oldMaxKey);
      Preconditions.checkState(index > 0);
      Preconditions.checkState(children[index] == child);
      keys[index] = newMaxKey;
      if (index == count - 1 && parent != null) {
        parent.updateChildMaxKey(this, oldMaxKey, newMaxKey);
      }
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
        arrayShift(keys, index, n);
        arrayShift(children, index, n);
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
            performLeafCompaction(depth, start, n);
            findAdjacentLeavesToCompact(start + 1, depth);
            return;
          } else {
            sumLeafSize = newSumLeafSize;
          }
        } else {
          performLeafCompaction(depth, start, n);
          ((ParentNode) child).findAdjacentLeavesToCompact(0, depth + 1);
          findAdjacentLeavesToCompact(i + 1 - Math.max(0, n - 1), depth);
          return;
        }
      }
      performLeafCompaction(depth, start, count - start);
    }

    private void performLeafCompaction(int depth, int start, int n) {
      if (n <= 1) {
        return;
      }
      print(depth, "performLeafCompaction", hashCode(), start, n, count);
      var initialSize = size();

      Arrays.stream(children, start, start + n).forEach(c -> print(depth, "cmpt sz=" + c.size(), c.hashCode()));

      LeafNode.compact(children, start, n);
      keys[start] = children[start].maxKey();
      print(depth, "combined size", children[start].size());

      Arrays.stream(children, start + n, count).forEach(c -> print(depth, "lft sz=" + c.size(), c.hashCode()));

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

      Arrays.stream(children, 0, count).forEach(c -> print(depth, "fnl sz=" + c.size(), c.hashCode()));

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

  /**
   * Don't use {@link System#arraycopy} because it makes a temporary array when src and dst are the same
   */
  private static void arrayShift(Object[] array, int index, int count) {
    Preconditions.checkState(index >= 0 && index + count < array.length && count >= 0);
    for (int i = index + count; i > index; i--) {
      array[i] = array[i - 1];
    }
  }
}

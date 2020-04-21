package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.stream.Stream;

class ParentNode<K extends Comparable<K>, V> extends Node<K, V> {
  // use boxed array so that it can be passed to generic Object[] methods
  private final Long[] children;

  ParentNode(int capacity, @NotNull BTreeStorage<K, V> storage) {
    super(capacity, storage);
    this.children = new Long[capacity];
  }

  @VisibleForTesting ParentNode(int count, @NotNull K[] keys, @NotNull Long[] children,
                                @NotNull BTreeStorage<K, V> storage) {
    super(count, keys, storage);
    Preconditions.checkState(keys.length == children.length);
    this.children = children;
  }

  private Stream<Node<K, V>> streamChildren() {
    return Arrays.stream(children).limit(count).map(storage::getNode);
  }

  private Node<K, V> getChild(int index) {
    Preconditions.checkArgument(index < count);
    return storage.getNode(children[index]);
  }

  @Override public int size() {
    return streamChildren().mapToInt(Node::size).sum();
  }

  @Override public K maxKey() {
    Preconditions.checkState(count > 0);
    return keys[count - 1];
  }

  @Override public K minKey() {
    Preconditions.checkState(count > 0);
    return getChild(0).minKey();
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
    return getChild(index).get(key);
  }

  @Override public Node<K, V> put(K key, V value) {
    Preconditions.checkState(count > 0);
    if (false) {
      System.out.println(Joiner.on(" ").join("parent put",
          key,
          "sz=" + size(),
          this
      ));
    }

    var index = binarySearch(key);
    if (index >= 0) {
      var child = getChild(index);
      var result = child.put(key, value);
      Preconditions.checkState(child == result, "should not split for existing maxKey");
      return this;
    }

    index = invertNegativeBinarySearchIndex(index);
    if (index == count) {
      return putNewMaxKey(key, value);
    }

    var child = getChild(index);
    Preconditions.checkState(child != this);
    Preconditions.checkState(child.parentId == this.id);

    // if key is in the range of this child, then we have to give it to that child
    if (child.minKey().compareTo(key) <= 0 && child.maxKey().compareTo(key) >= 0) {
      var result = child.put(key, value);
      // put can cause the child to split and it may reuse it's current parent node, which is `this`
      if (result != this && result != child) {
        // child.put(k,v) can lead to child.split().put(k, v)
        // if the split reuse this node then we recurse into call this.put again
        // if on this second call we're full, then we split and created a new node above us
        // hence it is possible for the result of child.put(k, v) to return a node that
        // is actually our new parent
        if (this.parentId == result.id) {
          return result;
        }
        addNode(result);
      }
      return this;
    }

    // if the child is not full, prefer to add entry to existing leaf child rather than creating new node
    if (child instanceof LeafNode && !child.isFull()) {
      Preconditions.checkState(key.compareTo(child.minKey()) < 0);
      var result = child.put(key, value);
      Preconditions.checkState(result == child);
      return this;
    }

    if (isFull()) {
      return putFull(key, value, child);
    }

    insertNode(new LeafNode<>(getCapacity(), this).put(key, value), index);
    return this;
  }

  @Override boolean delete(K key) {
    //System.out.println("delete key=" + key + " in " + this);
    Preconditions.checkState(count > 0);
    var index = binarySearch(key);
    //System.out.println("index " +  index);
    if (index < 0) {
      index = invertNegativeBinarySearchIndex(index);
      if (index == count) {
        return false;
      }
    }
    return getChild(index).delete(key);
  }

  @Override protected ParentNode<K, V> performSplit(int start, int end) {
    var newParent = new ParentNode<>(getCapacity(), storage);
    for (int i = start; i < end; i++) {
      var child = getChild(i);
      Preconditions.checkState(child.parentId == this.id);
      child.setParent(newParent);
    }
    splitHelper(start, end, newParent, children, newParent.children);
    return newParent;
  }

  void addNode(Node<K, V> node) {
    Preconditions.checkState(node.parentId == BTreeStorage.NO_ID || node.parentId == this.id);
    Preconditions.checkState(node.id != parentId, "%s cannot contain its parent", this);
    node.setParent(this);
    var maxKey = node.maxKey();
    if (count == 0) {
      keys[0] = maxKey;
      children[0] = node.id;
      count++;
      return;
    }
    var index = binarySearch(maxKey);
    Preconditions.checkState(index < 0);
    insertNode(node, -(index + 1));
  }

  void removeNode(Node<K, V> node) {
    removeChildInternal(node, node.maxKey());
  }

  void updateChildMaxKey(Node<K, V> child, K oldMaxKey, K newMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    keys[index] = newMaxKey;
  }

  void handleChildDeleteKey(Node<K, V> child, K oldMaxKey, K newMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    if (!newMaxKey.equals(oldMaxKey)) {
      keys[index] = newMaxKey;
      if (index == count - 1) {
        optionalParent().ifPresent(parent -> parent.handleChildDeleteKey(this, oldMaxKey, newMaxKey));
      }
    }
    if (child instanceof LeafNode) {
      checkCombineAdjacentLeaves(index);
    }
  }

  void deleteChild(Node<K, V> child, K oldMaxKey) {
    if (count == 0) {
      storage.deleteNode(child.id);
    }
    int index = removeChildInternal(child, oldMaxKey);
    optionalParent().ifPresent(parent -> {
      if (this.count == 0) {
        parent.deleteChild(this, oldMaxKey);

      } else if (index == this.count) {
        parent.handleChildDeleteKey(this, oldMaxKey, maxKey());
      }
    });
  }

  private int removeChildInternal(Node<K, V> child, K oldMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    child.parentId = BTreeStorage.NO_ID;
    int n = this.count - index - 1;
    if (n > 0) {
      arrayShiftDown(keys, index, n);
      arrayShiftDown(children, index, n);
    } else {
      keys[index] = null;
      children[index] = null;
    }
    this.count--;
    return index;
  }

  private int getChildIndex(Node<K, V> child, K key) {
    Preconditions.checkState(child.parentId == this.id);
    var index = binarySearch(key);
    Preconditions.checkState(index >= 0, "no key %s in %s", key, this);
    Preconditions.checkState(children[index] == child.id);
    return index;
  }

  private Node<K, V> putNewMaxKey(K key, V value) {
    Node<K, V> result;
    var oldMaxKey = keys[count - 1];
    var child = getChild(count - 1);
    if (!child.isFull()) {
      var putResult = child.put(key, value);
      Preconditions.checkState(putResult == child);
      keys[count - 1] = key;
      result = this;
    } else if (isFull()) {
      result = putFull(key, value, child);
    } else {
      var newChild = new LeafNode<K, V>(getCapacity(), this).put(key, value);
      children[count] = newChild.id;
      keys[count] = key;
      count++;
      result = this;
    }
    if (result == this) {
      optionalParent().ifPresent(parent -> parent.updateChildMaxKey(this, oldMaxKey, key));
    }
    return result;
  }

  private Node<K, V> putFull(K key, V value, Node<K, V> child) {
    // splitting at capacity 2 won't allow us to insert a new leaf node since we'll still have
    // two children. instead have to push the key down to a LeafNode
    if (getCapacity() == 2) {
      var result = child.put(key, value);
      if (result != child) {
        addNode(result);
      }
      return this;
    }
    return split(key).put(key, value);
  }

  private void insertNode(Node<K, V> node, int index) {
    Preconditions.checkState(node.parentId == this.id);
    var n = count - index;
    if (n > 0) {
      arrayShiftUp(keys, index, n);
      arrayShiftUp(children, index, n);
    }
    keys[index] = node.maxKey();
    children[index] = node.id;
    count++;
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
          combinedSize += getChild(k).size();
        }
        if (combinedSize <= getCapacity() && combinedSize > bestCombinedSize) {
          bestCombinedSize = combinedSize;
          bestStart = i;
          bestEnd = j;
        }
      }
    }
    if (bestCombinedSize != 0) {
      combineAdjacentLeaves(bestStart, bestEnd - bestStart + 1);
    }
  }

  int searchAdjacentLeavesToCombine(int start, int inclusiveEnd) {
    int offset = start < inclusiveEnd ? 1 : -1;
    for (int i = start + offset; i != inclusiveEnd + offset; i += offset) {
      if (!(getChild(i) instanceof LeafNode)) {
        return i - offset;
      }
    }
    return inclusiveEnd;
  }

  private void combineAdjacentLeaves(int start, int n) {
    if (n <= 1) {
      return;
    }
    var initialSize = size();

    var combined = (LeafNode<K, V>) getChild(start);
    LeafNode.combine(combined,
        Arrays.stream(children, start + 1, start + n)
            .map(storage::getNode)
            .map(x -> (LeafNode<K, V>) x)
            .iterator());
    keys[start] = combined.maxKey();

    int removed = n - 1;
    int endShift = count - removed;
    for (int i = start + 1; i < endShift; i++) {
      int j = i + removed;
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
      getChild(i).recursivelyPrint(depth + 1);
      System.out.println(prefix + keys[i]);
    }
  }

  @Override public String toString() {
    return baseToStringHelper()
        .add("children",
            "[" + Joiner.on(",").join(Arrays.stream(children).limit(count)
                .map(x -> x == null ? "null" : x)
                .iterator()) + "]")
        .toString();
  }
}

package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import dbf0.common.Dbf0Util;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Stream;

class ParentNode<K extends Comparable<K>, V> extends Node<K, V> {
  private static final Logger LOGGER = Dbf0Util.getLogger(ParentNode.class);

  // use boxed array so that it can be passed to generic Object[] methods
  private final Long[] childIds;

  ParentNode(long id, int capacity, @NotNull BTreeStorage<K, V> storage) {
    super(id, capacity, storage);
    this.childIds = new Long[capacity];
  }

  @VisibleForTesting ParentNode(long id, int count, @NotNull K[] keys, @NotNull Long[] childIds,
                                @NotNull BTreeStorage<K, V> storage) {
    super(id, BTreeStorage.NO_ID, count, keys, storage);
    Preconditions.checkState(keys.length == childIds.length);
    this.childIds = childIds;
  }

  ParentNode(long id, long parentId, int count, @NotNull K[] keys, @NotNull Long[] childIds,
             @NotNull BTreeStorage<K, V> storage) {
    super(id, parentId, count, keys, storage);
    Preconditions.checkState(keys.length == childIds.length);
    this.childIds = childIds;
  }

  private Stream<Node<K, V>> streamChildren() {
    return Arrays.stream(childIds).limit(count).map(storage::getNode);
  }

  private Node<K, V> getChild(int index) {
    Preconditions.checkArgument(index < count);
    return storage.getNode(childIds[index]);
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
        .add("children", childIds == null ? "<null>" :
            "[" + Joiner.on(",").join(Arrays.stream(childIds).limit(count)
                .map(x -> x == null ? "null" : x)
                .iterator()) + "]")
        .toString();
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
    LOGGER.finer(() -> Joiner.on(" ").join("parent put",
        key,
        // "sz=" + size(),
        this
    ));

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
    Preconditions.checkState(child.getParentId() == this.id);

    // if key is in the range of this child, then we have to give it to that child
    if (child.minKey().compareTo(key) <= 0 && child.maxKey().compareTo(key) >= 0) {
      return selectPutResult(child.put(key, value));
    }

    // if the child is not full, prefer to add entry to existing leaf child rather than creating new node
    if (child instanceof LeafNode && !child.isFull()) {
      Preconditions.checkState(key.compareTo(child.minKey()) < 0);
      var result = child.put(key, value);
      Preconditions.checkState(result == child);
      nodeChanged();
      return this;
    }

    if (isFull()) {
      return putFull(key, value, child);
    }

    insertNode(createLeaf(key, value), index);
    return this;
  }

  @Override boolean delete(K key) {
    //System.out.println("delete key=" + key + " in " + this);
    Preconditions.checkState(count > 0);
    var index = binarySearch(key);
    if (index < 0) {
      index = invertNegativeBinarySearchIndex(index);
      if (index == count) {
        return false;
      }
    }
    var deleted = getChild(index).delete(key);
    if (deleted && count == 0) {
      storage.deleteNode(id);
    }
    return deleted;
  }

  @Override protected ParentNode<K, V> performSplit(int start, int end) {
    var newParent = storage.createParent();
    for (int i = start; i < end; i++) {
      var child = getChild(i);
      Preconditions.checkState(child.getParentId() == this.id);
      child.setParent(newParent);
    }
    splitHelper(start, end, newParent, childIds, newParent.childIds);
    return newParent;
  }

  void addNode(Node<K, V> node) {
    Preconditions.checkState(node.getParentId() == BTreeStorage.NO_ID || node.getParentId() == this.id);
    Preconditions.checkState(node.id != getParentId(), "%s cannot contain its parent", this);
    node.setParent(this);
    var maxKey = node.maxKey();
    if (count == 0) {
      keys[0] = maxKey;
      childIds[0] = node.id;
      count++;
      return;
    }
    var index = binarySearch(maxKey);
    if (index >= 0) {
      var existing = getChild(index);
      throw new IllegalStateException(String.format("%s already contains %s at %s as %s",
          this, node, index, existing));
    } else {
      insertNode(node, invertNegativeBinarySearchIndex(index));
    }
  }

  void removeNode(Node<K, V> node) {
    removeChildInternal(node, node.maxKey());
  }

  void updateChildMaxKey(Node<K, V> child, K oldMaxKey, K newMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    keys[index] = newMaxKey;
    nodeChanged();
  }

  void handleChildDeleteKey(Node<K, V> child, K oldMaxKey, K newMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    var changed = false;
    if (!newMaxKey.equals(oldMaxKey)) {
      keys[index] = newMaxKey;
      changed = true;
      if (index == count - 1) {
        optionalParent().ifPresent(parent -> parent.handleChildDeleteKey(this, oldMaxKey, newMaxKey));
      }
    }
    if (child instanceof LeafNode) {
      checkCombineAdjacentLeaves(index);
      changed = true;
    }
    if (changed) {
      nodeChanged();
    }
  }

  void deleteChild(Node<K, V> child, K oldMaxKey) {
    int index = removeChildInternal(child, oldMaxKey);
    optionalParent().ifPresent(parent -> {
      if (count == 0) {
        parent.deleteChild(this, oldMaxKey);
      } else if (index == count) {
        parent.handleChildDeleteKey(this, oldMaxKey, maxKey());
      }
    });
  }

  Stream<Long> streamIdsInUse() {
    return Streams.concat(
        Arrays.stream(childIds).limit(count),
        streamChildren()
            .filter(ParentNode.class::isInstance)
            .map(ParentNode.class::cast)
            .flatMap(ParentNode::streamIdsInUse)
    );
  }

  List<Long> getChildIds() {
    return Arrays.asList(childIds).subList(0, count);
  }

  @NotNull private LeafNode<K, V> createLeaf(K key, V value) {
    var leaf = storage.createLeaf();
    leaf.setParent(this);
    leaf.put(key, value);
    return leaf;
  }

  private int removeChildInternal(Node<K, V> child, K oldMaxKey) {
    int index = getChildIndex(child, oldMaxKey);
    child.clearParent();
    int n = this.count - index - 1;
    if (n > 0) {
      arrayShiftDown(keys, index, n);
      arrayShiftDown(childIds, index, n);
    } else {
      keys[index] = null;
      childIds[index] = null;
    }
    this.count--;
    // count==0 for deletion is handled in delete(key)
    if (count > 0) {
      nodeChanged();
    }
    return index;
  }

  private int getChildIndex(Node<K, V> child, K key) {
    Preconditions.checkState(child.getParentId() == this.id);
    var index = binarySearch(key);
    Preconditions.checkState(index >= 0, "no key %s in %s", key, this);
    Preconditions.checkState(childIds[index] == child.id);
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
      nodeChanged();
      result = this;
    } else if (isFull()) {
      result = putFull(key, value, child);
    } else {
      var newChild = createLeaf(key, value);
      childIds[count] = newChild.id;
      keys[count] = key;
      count++;
      nodeChanged();
      result = this;
    }
    if (result == this) {
      optionalParent().ifPresent(parent -> parent.updateChildMaxKey(this, oldMaxKey, key));
    }
    return result;
  }

  private Node<K, V> selectPutResult(Node<K, V> result) {
    // the child can be split an arbitrary number of times, and these splits can cascade up
    // and involve not just splitting this node, but possibly its parent(s) as well
    // hence we need we're only the result node if the result of putting has us as the parent,
    // including if the result is the current child
    return result.getParentId() == id ? this : result;
  }

  private Node<K, V> putFull(K key, V value, Node<K, V> child) {
    // splitting at capacity 2 won't allow us to insert a new leaf node since we'll still have
    // two children. instead have to push the key down to a LeafNode
    if (getCapacity() == 2) {
      return selectPutResult(child.put(key, value));
    }
    return split(key).put(key, value);
  }

  private void insertNode(Node<K, V> node, int index) {
    Preconditions.checkState(node.getParentId() == this.id);
    var n = count - index;
    if (n > 0) {
      arrayShiftUp(keys, index, n);
      arrayShiftUp(childIds, index, n);
    }
    keys[index] = node.maxKey();
    childIds[index] = node.id;
    count++;
    nodeChanged();
  }

  private void checkCombineAdjacentLeaves(int index) {
    int start = searchAdjacentLeavesToCombine(index, 0);
    int end = searchAdjacentLeavesToCombine(index, count - 1);
    if (start == end) {
      return;
    }
    int bestStart = -1;
    int bestEnd = -1;
    int bestCombinedSize = 0;
    var leafCapacity = storage.getConfig().getLeafCapacity();
    for (int i = start; i < end; i++) {
      for (int j = i + 1; j <= end; j++) {
        int combinedSize = 0;
        for (int k = i; k <= j; k++) {
          combinedSize += getChild(k).size();
        }
        if (combinedSize <= leafCapacity && combinedSize > bestCombinedSize) {
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

  private int searchAdjacentLeavesToCombine(int start, int inclusiveEnd) {
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
        Arrays.stream(childIds, start + 1, start + n)
            .map(storage::getNode)
            .map(x -> (LeafNode<K, V>) x)
    );
    keys[start] = combined.maxKey();

    //Arrays.stream(childIds, start + 1, start + n).forEach(storage::deleteNode);

    int removed = n - 1;
    int endShift = count - removed;
    for (int i = start + 1; i < endShift; i++) {
      int j = i + removed;
      Preconditions.checkState(j < count);
      Preconditions.checkState(keys[j] != null);
      Preconditions.checkState(childIds[j] != null);
      keys[i] = keys[j];
      childIds[i] = childIds[j];
    }
    for (int i = count - removed; i < count; i++) {
      keys[i] = null;
      childIds[i] = null;
    }
    count -= removed;
    nodeChanged();

    Preconditions.checkState(count > 0);
    Preconditions.checkState(keys[count - 1] != null);
    Preconditions.checkState(size() == initialSize, "%s!=%s", initialSize, size());
  }
}

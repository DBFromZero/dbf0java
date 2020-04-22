package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import dbf0.common.ByteArrayWrapper;
import dbf0.disk_key_value.readwrite.blocks.ByteSerializationHelper;
import dbf0.disk_key_value.readwrite.blocks.DeserializationHelper;
import dbf0.disk_key_value.readwrite.blocks.SerializationPair;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class NodeSerialization<K extends Comparable<K>, V> {

  public static final byte PARENT = (byte) 10;
  public static final byte LEAF = (byte) 20;
  public static final Map<Class<? extends Node>, Byte> NODE_CLASS_TO_BYTE = ImmutableMap.of(
      ParentNode.class, PARENT,
      LeafNode.class, LEAF
  );

  private final int capacity;
  private final SerializationPair<K> keySerializationPair;
  private final SerializationPair<V> valueSerializationPair;

  public NodeSerialization(int capacity,
                           SerializationPair<K> keySerializationPair,
                           SerializationPair<V> valueSerializationPair) {
    this.capacity = capacity;
    this.keySerializationPair = keySerializationPair;
    this.valueSerializationPair = valueSerializationPair;
  }

  ByteArrayWrapper serialize(Node<K, V> node) throws IOException {
    Preconditions.checkState(node.getCapacity() == capacity);
    var helper = new ByteSerializationHelper();
    helper.writeLong(node.getId());
    helper.writeLong(node.getParentId());
    helper.writeInt(node.getCount());
    for (K key : node.getKeys()) {
      keySerializationPair.getSerializer().serialize(helper, key);
    }
    helper.writeByte(NODE_CLASS_TO_BYTE.get(node.getClass()));
    if (node instanceof LeafNode) {
      for (V value : ((LeafNode<K, V>) node).getValues()) {
        valueSerializationPair.getSerializer().serialize(helper, value);
      }
    } else {
      for (long childId : ((ParentNode<K, V>) node).getChildIds()) {
        helper.writeLong(childId);
      }
    }
    return helper.getBytes();
  }

  Node<K, V> deserialize(ByteArrayWrapper bw, BTreeStorage<K, V> storage) throws IOException {
    var helper = new DeserializationHelper(new ByteArrayInputStream(bw.getArray()));
    var nodeId = helper.readLong();
    var parentId = helper.readLong();
    int count = helper.readInt();
    K[] keys = (K[]) new Comparable[capacity];
    for (int i = 0; i < count; i++) {
      keys[i] = keySerializationPair.getDeserializer().desserialize(helper);
    }
    var type = helper.readByte();
    switch (type) {
      case LEAF:
        var values = (V[]) new Object[capacity];
        for (int i = 0; i < count; i++) {
          values[i] = valueSerializationPair.getDeserializer().desserialize(helper);
        }
        return new LeafNode<>(nodeId, parentId, count, keys, values, storage);
      case PARENT:
        var childIds = new Long[capacity];
        for (int i = 0; i < count; i++) {
          childIds[i] = helper.readLong();
        }
        return new ParentNode<K, V>(nodeId, parentId, count, keys, childIds, storage);
      default:
        throw new RuntimeException("Bad node type " + type);
    }
  }
}

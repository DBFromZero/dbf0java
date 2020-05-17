package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import dbf0.disk_key_value.io.DeprecatedSerializationHelper;
import dbf0.disk_key_value.io.DeserializationHelper;
import dbf0.disk_key_value.io.SerializationPair;

import java.io.IOException;
import java.util.Map;

public class NodeSerialization<K extends Comparable<K>, V> {

  public static final byte PARENT = (byte) 10;
  public static final byte LEAF = (byte) 20;
  public static final Map<Class<? extends Node>, Byte> NODE_CLASS_TO_BYTE = ImmutableMap.of(
      ParentNode.class, PARENT,
      LeafNode.class, LEAF
  );

  private final BTreeConfig config;
  private final SerializationPair<K> keySerializationPair;
  private final SerializationPair<V> valueSerializationPair;

  public NodeSerialization(BTreeConfig config,
                           SerializationPair<K> keySerializationPair,
                           SerializationPair<V> valueSerializationPair) {
    this.config = config;
    this.keySerializationPair = keySerializationPair;
    this.valueSerializationPair = valueSerializationPair;
  }

  void serialize(DeprecatedSerializationHelper helper, Node<K, V> node) throws IOException {
    var type = NODE_CLASS_TO_BYTE.get(node.getClass());
    Preconditions.checkArgument(type != null, "bad node type %s", node);
    helper.writeByte(type);
    helper.writeLong(node.getId());
    helper.writeLong(node.getParentId());
    helper.writeInt(node.getCount());
    for (K key : node.getKeys()) {
      keySerializationPair.getSerializer().serialize(helper, key);
    }
    if (node instanceof LeafNode) {
      for (V value : ((LeafNode<K, V>) node).getValues()) {
        valueSerializationPair.getSerializer().serialize(helper, value);
      }
    } else {
      for (long childId : ((ParentNode<K, V>) node).getChildIds()) {
        helper.writeLong(childId);
      }
    }
  }

  Node<K, V> deserialize(DeserializationHelper helper, BTreeStorage<K, V> storage) throws IOException {
    var type = helper.readByte();
    Integer capacity = Map.of(LEAF, config.getLeafCapacity(), PARENT, config.getParentCapacity()).get(type);
    Preconditions.checkState(capacity != null, "bad node type %s", type);
    var nodeId = helper.readLong();
    var parentId = helper.readLong();
    int count = helper.readInt();
    K[] keys = (K[]) new Comparable[capacity];
    for (int i = 0; i < count; i++) {
      keys[i] = keySerializationPair.getDeserializer().deserialize(helper);
    }
    switch (type) {
      case LEAF:
        var values = (V[]) new Object[capacity];
        for (int i = 0; i < count; i++) {
          values[i] = valueSerializationPair.getDeserializer().deserialize(helper);
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

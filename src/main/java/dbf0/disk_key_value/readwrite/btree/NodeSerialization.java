package dbf0.disk_key_value.readwrite.btree;

import dbf0.common.ByteArrayWrapper;
import dbf0.disk_key_value.readwrite.blocks.SerializationPair;

import java.io.IOException;

public class NodeSerialization<K extends Comparable<K>, V> {

  private final SerializationPair<K> keySerializationPair;
  private final SerializationPair<V> valueSerializationPair;

  public NodeSerialization(SerializationPair<K> keySerializationPair,
                           SerializationPair<V> valueSerializationPair) {
    this.keySerializationPair = keySerializationPair;
    this.valueSerializationPair = valueSerializationPair;
  }

  ByteArrayWrapper serialize(Node<K, V> node) throws IOException {

  }

  Node<K, V> deserialize(ByteArrayWrapper bw) throws IOException {

  }
}

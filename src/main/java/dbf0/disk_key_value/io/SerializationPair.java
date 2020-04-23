package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

public class SerializationPair<T> {

  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public SerializationPair(Serializer<T> serializer, Deserializer<T> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  public Serializer<T> getSerializer() {
    return serializer;
  }

  public Deserializer<T> getDeserializer() {
    return deserializer;
  }

  public static SerializationPair<Integer> intSerializationPair() {
    return new SerializationPair<>(Serializer.intSerializer(), Deserializer.intDeserializer());
  }

  public static SerializationPair<ByteArrayWrapper> bytesSerializationPair() {
    return new SerializationPair<>(Serializer.bytesSerializer(), Deserializer.bytesDeserializer());
  }
}

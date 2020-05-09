package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

@Deprecated
public class SerializationPair<T> {

  private final DeprecatedSerializer<T> serializer;
  private final DeprecatedDeserializer<T> deserializer;

  public SerializationPair(DeprecatedSerializer<T> serializer, DeprecatedDeserializer<T> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  public DeprecatedSerializer<T> getSerializer() {
    return serializer;
  }

  public DeprecatedDeserializer<T> getDeserializer() {
    return deserializer;
  }

  public static SerializationPair<Integer> intSerializationPair() {
    return new SerializationPair<>(DeprecatedSerializer.intSerializer(), DeprecatedDeserializer.intDeserializer());
  }

  public static SerializationPair<ByteArrayWrapper> bytesSerializationPair() {
    return new SerializationPair<>(DeprecatedSerializer.bytesSerializer(), DeprecatedDeserializer.bytesDeserializer());
  }
}

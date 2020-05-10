package dbf0.common.io;

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
}

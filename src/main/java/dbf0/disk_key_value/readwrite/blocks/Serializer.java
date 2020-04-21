package dbf0.disk_key_value.readwrite.blocks;

public interface Serializer<T> {
  void serialize(SerializationHelper helper, T x);
}

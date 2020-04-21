package dbf0.disk_key_value.readwrite.blocks;

public interface Deserializer<T> {
  T desserialize(DeserializationHelper helper);
}

package dbf0.disk_key_value.readwrite.blocks;

import java.io.IOException;

public interface Deserializer<T> {
  T desserialize(DeserializationHelper helper) throws IOException;

  static Deserializer<Integer> intDeserializer() {
    return DeserializationHelper::readInt;
  }
}

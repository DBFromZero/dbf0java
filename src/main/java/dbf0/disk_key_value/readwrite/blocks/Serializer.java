package dbf0.disk_key_value.readwrite.blocks;

import java.io.IOException;

public interface Serializer<T> {
  void serialize(SerializationHelper helper, T x) throws IOException;

  static Serializer<Integer> intSerializer() {
    return SerializationHelper::writeInt;
  }
}

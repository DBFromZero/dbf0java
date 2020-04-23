package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

import java.io.IOException;

public interface Deserializer<T> {
  T deserialize(DeserializationHelper helper) throws IOException;

  static Deserializer<Integer> intDeserializer() {
    return DeserializationHelper::readInt;
  }

  static Deserializer<ByteArrayWrapper> bytesDeserializer() {
    return DeserializationHelper::readBytes;
  }
}

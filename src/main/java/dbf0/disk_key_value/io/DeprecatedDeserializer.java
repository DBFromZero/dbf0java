package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

import java.io.IOException;

@Deprecated
public interface DeprecatedDeserializer<T> {
  T deserialize(DeserializationHelper helper) throws IOException;

  static DeprecatedDeserializer<Integer> intDeserializer() {
    return DeserializationHelper::readInt;
  }

  static DeprecatedDeserializer<ByteArrayWrapper> bytesDeserializer() {
    return DeserializationHelper::readBytes;
  }
}

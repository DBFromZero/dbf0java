package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

import java.io.IOException;

public interface Serializer<T> {
  void serialize(SerializationHelper helper, T x) throws IOException;

  static Serializer<Integer> intSerializer() {
    return SerializationHelper::writeInt;
  }

  static Serializer<Long> longSerializer() {
    return SerializationHelper::writeLong;
  }

  static Serializer<ByteArrayWrapper> bytesSerializer() {
    return SerializationHelper::writeBytes;
  }
}

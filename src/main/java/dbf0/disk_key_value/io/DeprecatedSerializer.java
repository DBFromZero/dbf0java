package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

import java.io.IOException;

@Deprecated
public interface DeprecatedSerializer<T> {
  void serialize(DeprecatedSerializationHelper helper, T x) throws IOException;

  static DeprecatedSerializer<Integer> intSerializer() {
    return DeprecatedSerializationHelper::writeInt;
  }

  static DeprecatedSerializer<Long> longSerializer() {
    return DeprecatedSerializationHelper::writeLong;
  }

  static DeprecatedSerializer<ByteArrayWrapper> bytesSerializer() {
    return DeprecatedSerializationHelper::writeBytes;
  }
}

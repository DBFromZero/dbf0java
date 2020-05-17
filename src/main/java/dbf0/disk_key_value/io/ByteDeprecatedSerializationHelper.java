package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

import java.io.ByteArrayOutputStream;

@Deprecated
public class ByteDeprecatedSerializationHelper extends DeprecatedSerializationHelper {

  public ByteDeprecatedSerializationHelper(int size) {
    super(new ByteArrayOutputStream(size));
  }

  public ByteDeprecatedSerializationHelper() {
    this(2048);
  }

  public ByteArrayWrapper getBytes() {
    return ByteArrayWrapper.of(((ByteArrayOutputStream) outputStream).toByteArray());
  }
}

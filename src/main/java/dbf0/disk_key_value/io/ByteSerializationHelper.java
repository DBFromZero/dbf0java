package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;

import java.io.ByteArrayOutputStream;

public class ByteSerializationHelper extends SerializationHelper {

  public ByteSerializationHelper(int size) {
    super(new ByteArrayOutputStream(size));
  }

  public ByteSerializationHelper() {
    this(2048);
  }

  public ByteArrayWrapper getBytes() {
    return ByteArrayWrapper.of(((ByteArrayOutputStream) outputStream).toByteArray());
  }
}

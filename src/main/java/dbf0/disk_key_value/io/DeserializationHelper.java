package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static dbf0.disk_key_value.io.DeprecatedSerializationHelper.INT_BYTES;
import static dbf0.disk_key_value.io.DeprecatedSerializationHelper.LONG_BYTES;

public class DeserializationHelper {

  protected final InputStream inputStream;

  public DeserializationHelper(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  public DeserializationHelper(byte[] bytes) {
    this(new ByteArrayInputStream(bytes));
  }

  public DeserializationHelper(ByteArrayWrapper bw) {
    this(bw.getArray());
  }

  public long readLong() throws IOException {
    return readByteBuffer(LONG_BYTES).getLong();
  }

  public int readInt() throws IOException {
    return readByteBuffer(INT_BYTES).getInt();
  }

  public byte readByte() throws IOException {
    return (byte) inputStream.read();
  }

  public ByteArrayWrapper readBytes() throws IOException {
    return IOUtil.readBytes(inputStream);
  }

  private ByteBuffer readByteBuffer(int length) throws IOException {
    var bytes = new byte[length];
    IOUtil.readArrayFully(inputStream, bytes);
    return ByteBuffer.wrap(bytes);
  }
}

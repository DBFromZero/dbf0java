package dbf0.disk_key_value.readwrite.blocks;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.PrefixIo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static dbf0.disk_key_value.readwrite.blocks.SerializationHelper.INT_BYTES;
import static dbf0.disk_key_value.readwrite.blocks.SerializationHelper.LONG_BYTES;

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
    return PrefixIo.readBytes(inputStream);
  }

  private ByteBuffer readByteBuffer(int length) throws IOException {
    var bytes = new byte[length];
    Dbf0Util.readArrayFully(inputStream, bytes);
    return ByteBuffer.wrap(bytes);
  }
}

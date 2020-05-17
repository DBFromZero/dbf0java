package dbf0.disk_key_value.io;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.IOUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

@Deprecated
public class DeprecatedSerializationHelper {

  public static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  public static final int INT_BYTES = Integer.SIZE / Byte.SIZE;
  protected final OutputStream outputStream;

  public DeprecatedSerializationHelper(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  //TODO: More space efficient way of writing numbers

  public void writeLong(long l) throws IOException {
    outputStream.write(ByteBuffer.allocate(LONG_BYTES).putLong(l).array());
  }

  public void writeInt(int i) throws IOException {
    outputStream.write(ByteBuffer.allocate(INT_BYTES).putInt(i).array());
  }

  public void writeByte(byte b) throws IOException {
    outputStream.write(b);
  }

  public void writeBytes(ByteArrayWrapper w) throws IOException {
    IOUtil.writeBytes(outputStream, w);
  }

  public void writeString(String s) throws IOException {
    writeBytes(ByteArrayWrapper.of(s.getBytes(Charset.defaultCharset())));
  }
}

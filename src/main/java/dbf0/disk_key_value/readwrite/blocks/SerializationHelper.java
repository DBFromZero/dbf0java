package dbf0.disk_key_value.readwrite.blocks;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.PrefixIo;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SerializationHelper {

  public static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  public static final int INT_BYTES = Integer.SIZE / Byte.SIZE;
  protected final OutputStream outputStream;

  public SerializationHelper(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  public void writeLong(long l) throws IOException {
    outputStream.write(ByteBuffer.allocate(LONG_BYTES).putLong(l).array());
  }

  public void writeInt(int i) throws IOException {
    outputStream.write(ByteBuffer.allocate(INT_BYTES).putInt(i).array());
  }

  public void writeBytes(ByteArrayWrapper w) throws IOException {
    PrefixIo.writeBytes(outputStream, w);
  }
}

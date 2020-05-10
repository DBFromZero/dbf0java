package dbf0.common.io;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class ByteArraySerializer implements Serializer<ByteArrayWrapper> {

  private static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

  @NotNull public static ByteArraySerializer getInstance() {
    return INSTANCE;
  }

  private ByteArraySerializer() {
  }

  @Override public void serialize(OutputStream s, ByteArrayWrapper x) throws IOException {
    var a = x.getArray();
    IOUtil.writeVariableLengthUnsignedInt(s, x.length());
    s.write(a);
  }

  @Override public int estimateSerializedSize(ByteArrayWrapper x) {
    var a = x.getArray();
    return IOUtil.sizeUnsignedLong(a.length) + a.length;
  }
}

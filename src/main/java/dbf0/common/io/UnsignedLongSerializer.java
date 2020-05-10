package dbf0.common.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class UnsignedLongSerializer implements Serializer<Long> {

  private static final UnsignedLongSerializer INSTANCE = new UnsignedLongSerializer();

  @NotNull public static UnsignedLongSerializer getInstance() {
    return INSTANCE;
  }

  private UnsignedLongSerializer() {
  }

  @Override public void serialize(OutputStream s, Long x) throws IOException {
    IOUtil.writeVariableLengthUnsignedLong(s, x);
  }

  @Override public int estimateSerializedSize(Long x) {
    return IOUtil.sizeUnsignedLong(x);
  }
}

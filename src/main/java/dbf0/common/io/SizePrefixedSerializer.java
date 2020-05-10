package dbf0.common.io;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SizePrefixedSerializer<T> implements Serializer<T> {

  private final Serializer<T> serializer;

  public SizePrefixedSerializer(Serializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override public void serialize(OutputStream s, T x) throws IOException {
    ByteArrayOutputStream buffer = serializeToBuffer(x);
    IOUtil.writeVariableLengthUnsignedInt(s, buffer.size());
    buffer.writeTo(s);
  }

  @Override public int estimateSerializedSize(T x) {
    int estimatedSize = serializer.estimateSerializedSize(x);
    return estimatedSize == SIZE_UNKNOWN ? SIZE_UNKNOWN : IOUtil.sizeUnsignedLong(estimatedSize) + estimatedSize;
  }

  @NotNull @Override public ByteArrayWrapper serializeToBytes(T x) throws IOException {
    return ByteArrayWrapper.of(serializeToBuffer(x).toByteArray());
  }

  @NotNull private ByteArrayOutputStream serializeToBuffer(T x) throws IOException {
    int estimatedSize = serializer.estimateSerializedSize(x);
    var buffer = new ByteArrayOutputStream(estimatedSize == SIZE_UNKNOWN ? 1024 : estimatedSize);
    serializer.serialize(buffer, x);
    return buffer;
  }

  @Override public boolean canBeDeserializedAsByteArrays() {
    return true;
  }
}

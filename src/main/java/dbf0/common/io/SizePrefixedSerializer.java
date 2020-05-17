package dbf0.common.io;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SizePrefixedSerializer<T> implements Serializer<T> {

  private final Serializer<T> serializer;
  private final ThreadLocal<ByteArrayOutputStream> outputStream =
      ThreadLocal.withInitial(() -> new ByteArrayOutputStream(2048));

  public SizePrefixedSerializer(Serializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override public void serialize(OutputStream s, T x) throws IOException {
    var buffer = serializeToBuffer(x);
    IOUtil.writeVariableLengthUnsignedInt(s, buffer.size());
    buffer.writeTo(s);
  }

  @Override public int estimateSerializedSize(T x) {
    int estimatedSize = serializer.estimateSerializedSize(x);
    return estimatedSize == SIZE_UNKNOWN ? SIZE_UNKNOWN : IOUtil.sizeUnsignedLong(estimatedSize) + estimatedSize;
  }

  // Exclude length prefix when serializing directly to bytes
  @NotNull @Override public ByteArrayWrapper serializeToBytes(T x) throws IOException {
    return ByteArrayWrapper.of(serializeToBuffer(x).toByteArray());
  }

  @NotNull private ByteArrayOutputStream serializeToBuffer(T x) throws IOException {
    var buffer = outputStream.get();
    buffer.reset();
    serializer.serialize(buffer, x);
    return buffer;
  }

  @Override public boolean isByteArrayEquivalent() {
    return true;
  }
}

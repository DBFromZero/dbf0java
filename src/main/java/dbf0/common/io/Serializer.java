package dbf0.common.io;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public interface Serializer<T> {

  int SIZE_UNKNOWN = -1;

  void serialize(OutputStream s, T x) throws IOException;

  /**
   * Returns the best estimate serialized size of element {@code x} in bytes
   * or {@code SIZE_UNKNOWN} if an estimate isn't possible. Errors on the side
   * of overestimating when there is uncertainty.
   */
  default int estimateSerializedSize(T x) {
    return SIZE_UNKNOWN;
  }

  @NotNull default ByteArrayWrapper serializeToBytes(T x) throws IOException {
    var estimatedSize = estimateSerializedSize(x);
    var stream = new ByteArrayOutputStream(estimatedSize == SIZE_UNKNOWN ? 1024 : estimatedSize);
    serialize(stream, x);
    return ByteArrayWrapper.of(stream.toByteArray());
  }
}

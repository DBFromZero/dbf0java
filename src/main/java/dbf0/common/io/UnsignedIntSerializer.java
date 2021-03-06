package dbf0.common.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class UnsignedIntSerializer implements Serializer<Integer> {

  private static final UnsignedIntSerializer INSTANCE = new UnsignedIntSerializer();

  @NotNull public static UnsignedIntSerializer getInstance() {
    return INSTANCE;
  }

  private static final SerializationPair<Integer> PAIR = new SerializationPair<>(
      UnsignedIntSerializer.getInstance(), UnsignedIntDeserializer.getInstance()
  );

  @NotNull public static SerializationPair<Integer> serializationPair() {
    return PAIR;
  }

  private UnsignedIntSerializer() {
  }

  @Override public void serialize(OutputStream s, Integer x) throws IOException {
    IOUtil.writeVariableLengthUnsignedInt(s, x);
  }

  @Override public int estimateSerializedSize(Integer x) {
    return IOUtil.sizeUnsignedLong(x);
  }
}

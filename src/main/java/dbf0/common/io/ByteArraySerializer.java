package dbf0.common.io;

import com.google.common.base.Suppliers;
import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

public class ByteArraySerializer implements Serializer<ByteArrayWrapper> {

  private static final ByteArraySerializer INSTANCE = new ByteArraySerializer();
  private static final Supplier<SerializationPair<ByteArrayWrapper>> BYTE_ARRAY_SERIALIZATION_PAIR_SUPPLIER
      = Suppliers.memoize(() -> new SerializationPair<>(getInstance(), ByteArrayDeserializer.getInstance()));

  @NotNull public static ByteArraySerializer getInstance() {
    return INSTANCE;
  }

  @NotNull public static SerializationPair<ByteArrayWrapper> serializationPair() {
    return BYTE_ARRAY_SERIALIZATION_PAIR_SUPPLIER.get();
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

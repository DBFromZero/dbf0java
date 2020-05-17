package dbf0.common.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

public class UnsignedLongDeserializer implements Deserializer<Long> {

  private static final UnsignedLongDeserializer INSTANCE = new UnsignedLongDeserializer();

  @NotNull public static UnsignedLongDeserializer getInstance() {
    return INSTANCE;
  }

  private UnsignedLongDeserializer() {
  }

  @NotNull @Override public Long deserialize(InputStream s) throws IOException {
    return IOUtil.readVariableLengthUnsignedLong(s);
  }
}

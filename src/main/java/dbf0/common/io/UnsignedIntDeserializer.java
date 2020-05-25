package dbf0.common.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

public class UnsignedIntDeserializer implements Deserializer<Integer> {

  private static final UnsignedIntDeserializer INSTANCE = new UnsignedIntDeserializer();

  @NotNull public static UnsignedIntDeserializer getInstance() {
    return INSTANCE;
  }

  private UnsignedIntDeserializer() {
  }

  @NotNull @Override public Integer deserialize(InputStream s) throws IOException {
    return IOUtil.readVariableLengthUnsignedInt(s);
  }
}

package dbf0.common.io;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

public class ByteArrayDeserializer implements Deserializer<ByteArrayWrapper> {

  @NotNull @Override public ByteArrayWrapper deserialize(InputStream s) throws IOException {
    int size = IOUtil.readVariableLengthUnsignedInt(s);
    var bytes = new byte[size];
    IOUtil.readArrayFully(s, bytes);
    return ByteArrayWrapper.of(bytes);
  }

  @Override public void skipDeserialize(InputStream s) throws IOException {
    IOUtil.skip(s, IOUtil.readVariableLengthUnsignedInt(s));
  }
}

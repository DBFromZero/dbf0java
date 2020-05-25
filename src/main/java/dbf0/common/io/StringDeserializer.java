package dbf0.common.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class StringDeserializer implements Deserializer<String> {

  private final Charset charset;

  public StringDeserializer(Charset charset) {
    this.charset = charset;
  }

  public StringDeserializer() {
    this(Charset.defaultCharset());
  }

  @NotNull @Override public String deserialize(InputStream s) throws IOException {
    int size = IOUtil.readVariableLengthUnsignedInt(s);
    var bytes = new byte[size];
    IOUtil.readArrayFully(s, bytes);
    return new String(bytes, charset);
  }

  @Override public void skipDeserialize(InputStream s) throws IOException {
    IOUtil.skip(s, IOUtil.readVariableLengthUnsignedInt(s));
  }
}

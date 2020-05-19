package dbf0.common.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class StringSerializer implements Serializer<String> {

  private final Charset charset;

  public StringSerializer(Charset charset) {
    this.charset = charset;
  }

  public StringSerializer() {
    this(Charset.defaultCharset());
  }

  @Override public void serialize(OutputStream s, String x) throws IOException {
    var a = x.getBytes(charset);
    IOUtil.writeVariableLengthUnsignedInt(s, x.length());
    s.write(a);
  }
}

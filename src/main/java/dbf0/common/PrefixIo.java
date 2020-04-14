package dbf0.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PrefixIo {

  // prefixes to identify different types of messages
  public static final byte SET = (byte) 's';
  public static final byte GET = (byte) 'g';
  public static final byte FOUND = (byte) 'f';
  public static final byte NOT_FOUND = (byte) 'n';


  public static void writePrefixLengthBytes(OutputStream s, ByteArrayWrapper w) throws IOException {
    var array = w.getArray();
    if (array.length > Byte.MAX_VALUE) {
      throw new RuntimeException("Byte array length " + array.length + " is too long");
    }
    s.write(array.length);
    s.write(array);
  }

  public static ByteArrayWrapper readPrefixLengthBytes(InputStream s) throws IOException {
    var length = s.read();
    if (length < 0) {
      throw new RuntimeException("Empty input stream");
    }
    var array = new byte[length];
    Dbf0Util.readArrayFully(s, array);
    return new ByteArrayWrapper(array);
  }
}

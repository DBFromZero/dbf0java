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
    writeLengthRec(s, array.length);
    if ((array.length & 0xFF) == 0xFF) {
      s.write(0);
    }
    s.write(array);
  }

  private static void writeLengthRec(OutputStream s, int remaining) throws IOException {
    int write = remaining & 0xFF;
    remaining = remaining >> 8;
    if (remaining > 0) {
      writeLengthRec(s, remaining);
    }
    s.write(write);
  }

  public static ByteArrayWrapper readPrefixLengthBytes(InputStream s) throws IOException {
    int length = 0;
    int next;
    do {
      next = s.read();
      if (next < 0) {
        throw new RuntimeException("Unexpected end of input stream");
      }
      if (next != 0) {
        length = length << 8;
        length += next;
      }
    } while (next == 0xFF);
    var array = new byte[length];
    Dbf0Util.readArrayFully(s, array);
    return new ByteArrayWrapper(array);
  }
}

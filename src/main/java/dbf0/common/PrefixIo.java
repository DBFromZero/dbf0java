package dbf0.common;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PrefixIo {

  // prefixes to identify different types of messages
  public static final byte SET = (byte) 's';
  public static final byte GET = (byte) 'g';
  public static final byte FOUND = (byte) 'f';
  public static final byte NOT_FOUND = (byte) 'n';

  @VisibleForTesting
  static final byte END_OF_LENGTH = (byte) 0x1A;
  @VisibleForTesting
  static final byte ESCAPE = (byte) 0x1B;


  public static void writeLength(OutputStream s, int length) throws IOException {
    int remaining = length;
    while (remaining > 0) {
      byte b = (byte) (remaining & 0xFF);
      if (b == END_OF_LENGTH || b == ESCAPE) {
        s.write(ESCAPE);
      }
      s.write(remaining & 0xFF);
      remaining = remaining >> 8;
    }
    s.write(END_OF_LENGTH);
  }

  public static int readLength(InputStream s) throws IOException {
    int length = 0, index = 0;
    while (true) {
      int next = s.read();
      if ((byte) next == END_OF_LENGTH) {
        break;
      }
      if ((byte) next == ESCAPE) {
        next = s.read();
      }
      if (next < 0) {
        throw new RuntimeException("Unexpected end of input stream");
      }
      length += (next << (8 * index++));
    }
    return length;
  }

  public static void writeBytes(OutputStream s, ByteArrayWrapper w) throws IOException {
    writeLength(s, w.length());
    s.write(w.getArray());
  }

  public static ByteArrayWrapper readBytes(InputStream s) throws IOException {
    int length = readLength(s);
    var array = new byte[length];
    Dbf0Util.readArrayFully(s, array);
    return new ByteArrayWrapper(array);
  }
}

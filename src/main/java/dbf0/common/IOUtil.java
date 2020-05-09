package dbf0.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOUtil {

  @VisibleForTesting static final int LOWER_7BITS_SET = 0x7F;
  @VisibleForTesting static final int EIGHTH_BIT_SET = 0x80;

  public static void writeVariableLengthUnsignedLong(OutputStream s, long l) throws IOException {
    Preconditions.checkState(l >= 0);
    long remaining;
    do {
      int b = ((int) l) & LOWER_7BITS_SET;
      remaining = l >>= 7;
      if (remaining > 0) {
        b = b | EIGHTH_BIT_SET;
      }
      s.write(b);
    } while (remaining > 0);
  }

  public static long readVariableLengthUnsignedLong(InputStream s) throws IOException {
    long value = 0;
    int index = 0;
    while (true) {
      int b = s.read();
      if (b < 0) {
        throw new EndOfStream("Unexpected end of input stream");
      }
      value += ((long) (b & LOWER_7BITS_SET)) << (index++ * 7);
      if ((b & EIGHTH_BIT_SET) == 0) {
        return value;
      }
    }
  }

  public static void writeVariableLengthUnsignedInt(OutputStream s, int i) throws IOException {
    writeVariableLengthUnsignedLong(s, i);
  }

  public static int readVariableLengthUnsignedInt(InputStream s) throws IOException {
    return Dbf0Util.safeLongToInt(readVariableLengthUnsignedLong(s));
  }

  public static void writeBytes(OutputStream s, ByteArrayWrapper w) throws IOException {
    writeVariableLengthUnsignedInt(s, w.length());
    s.write(w.getArray());
  }

  public static ByteArrayWrapper readBytes(InputStream s) throws IOException {
    int length = readVariableLengthUnsignedInt(s);
    var array = new byte[length];
    Dbf0Util.readArrayFully(s, array);
    return new ByteArrayWrapper(array);
  }
}

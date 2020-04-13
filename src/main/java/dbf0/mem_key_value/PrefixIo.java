package dbf0.mem_key_value;

import dbf0.ByteArrayWrapper;
import dbf0.Dbf0Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class PrefixIo {

  static final byte SET = (byte) 's';
  static final byte GET = (byte) 'g';
  static final byte FOUND = (byte) 'f';
  static final byte NOT_FOUND = (byte) 'n';

  static ByteArrayWrapper readPrefixLengthBytes(InputStream s) throws IOException {
    var length = s.read();
    if (length < 0) {
      throw new RuntimeException("Empty input stream");
    }
    var array = new byte[length];
    Dbf0Util.readArrayFully(s, array);
    return new ByteArrayWrapper(array);
  }

  static void writePrefixLengthBytes(OutputStream s, ByteArrayWrapper w) throws IOException {
    var array = w.getArray();
    if (array.length > Byte.MAX_VALUE) {
      throw new RuntimeException("Byte array length " + array.length + " is too long");
    }
    s.write(array.length);
    s.write(array);
  }
}

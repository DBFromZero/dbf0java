package dbf0;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class Dbf0Util {

  public static Logger getLogger(Class<?> c) {
    return Logger.getLogger(c.getName());
  }

  public static <T> T callUnchecked(Callable<T> callable) {
    try {
      return callable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void readArrayFully(InputStream s, byte[] bs) throws IOException {
    int i = 0, n;
    while (i < bs.length && (n = s.read(bs, i, bs.length - i)) != -1) {
      i += n;
    }
    if (i != bs.length) {
      throw new RuntimeException("Failed to read full message. Only read " + i + " bytes");
    }
  }
}

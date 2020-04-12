import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

public class Util {

  static <T> T callUnchecked(Callable<T> callable) {
    try {
      return callable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void readArrayFully(InputStream s, byte[] bs) throws IOException {
    int i = 0, n;
    while (i < bs.length && (n = s.read(bs, i, bs.length - i)) != -1) {
      i += n;
    }
    if (i != bs.length) {
      throw new RuntimeException("Failed to read full message. Only read " + i + " bytes");
    }
  }
}

package dbf0.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.logging.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Dbf0Util {

  public static Logger getLogger(Class<?> c) {
    return Logger.getLogger(c.getName());
  }

  public static void enableConsoleLogging(Level level) {
    var rootLogger = LogManager.getLogManager().getLogger("");
    var handler = new ConsoleHandler();
    Arrays.stream(rootLogger.getHandlers()).forEach(rootLogger::removeHandler);
    handler.setFormatter(new SimpleFormatter());
    rootLogger.addHandler(handler);
    rootLogger.setLevel(level);
    handler.setLevel(level);
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
      throw new EndOfStream("Failed to read full message. Only read " + i + " bytes");
    }
  }

  public static <T> Stream<T> iteratorStream(Iterator<T> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
  }
}

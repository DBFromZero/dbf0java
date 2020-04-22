package dbf0.common;

import com.google.common.collect.ImmutableList;

import java.io.File;
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

  public static ImmutableList<String> SIZE_SUFFIXES = ImmutableList.of("K", "M", "G", "T", "P", "X");

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

  public static void requireEmptyDirectory(String directory) {
    var d = new File(directory);
    if (d.isDirectory()) {
      var files = d.list().length;
      if (files > 0) {
        throw new RuntimeException("Directory " + directory + " is not empty. " + files + " found");
      }
    } else if (d.exists()) {
      throw new RuntimeException(directory + " is not a directory");
    } else if (!d.mkdirs()) {
      throw new RuntimeException("Failed to create directory");
    }
  }

  public static String formatSize(long size) {
    double scaled = size;
    var iter = SIZE_SUFFIXES.iterator();
    String suffix = "";
    while (scaled >= 100.0 && iter.hasNext()) {
      scaled /= 1000.0;
      suffix = iter.next();
    }
    return String.format("%.2f%s", scaled, suffix);
  }

  public static String formatBytes(long bytes) {
    return formatSize(bytes) + "B";
  }
}

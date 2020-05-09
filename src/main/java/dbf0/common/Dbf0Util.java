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
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Dbf0Util {

  public static Logger getLogger(Class<?> c) {
    return Logger.getLogger(c.getName());
  }

  public static ImmutableList<String> SIZE_SUFFIXES = ImmutableList.of("K", "M", "G", "T", "P", "X");

  public static void enableConsoleLogging(Level level) {
    enableConsoleLogging(level, false);
  }

  public static void enableConsoleLogging(Level level, boolean singleLine) {
    if (singleLine) {
      System.setProperty("java.util.logging.SimpleFormatter.format",
          "%1$tH:%1$tM:%1$tS.%1$tL %4$-6s %2$-60s %5$s%6$s%n");
    }
    var rootLogger = LogManager.getLogManager().getLogger("");
    var handler = new ConsoleHandler();
    Arrays.stream(rootLogger.getHandlers()).forEach(rootLogger::removeHandler);
    handler.setFormatter(singleLine ? new SingleLineLogFormatter() : new SimpleFormatter());
    rootLogger.addHandler(handler);
    rootLogger.setLevel(level);
    handler.setLevel(level);
  }

  public static class SingleLineLogFormatter extends SimpleFormatter {
    @Override public String format(LogRecord record) {
      record.setLoggerName(getSimpleName(record.getLoggerName()));
      if (record.getSourceClassName() != null) {
        record.setSourceClassName(getSimpleName(record.getSourceClassName()));
      }
      return super.format(record);
    }

    private static String getSimpleName(String classname) {
      var parts = classname.split(Pattern.quote("."));
      return parts[parts.length - 1];
    }
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

  public static int safeLongToInt(long l) {
    int i = (int) l;
    if (i != l) {
      throw new IllegalArgumentException("Cannot safely convert " + l + " to an integer");
    }
    return i;
  }
}

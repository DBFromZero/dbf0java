package dbf0.disk_key_value.readwrite.log;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.MemoryFileDirectoryOperations;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

public class WriteAheadLogTest {

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testDoNothing() throws Exception {
    var directoryOperations = new MemoryFileDirectoryOperations();
    var log = new WriteAheadLog<>(directoryOperations, ImmediateLogSynchronizer.factory());
    log.initialize(() -> {
      throw new AssertionError("should not be called");
    });
    assertThat(directoryOperations.list()).isEmpty();
  }

  @Test public void testFree() throws Exception {
    var directoryOperations = new MemoryFileDirectoryOperations();
    var initialLog = new WriteAheadLog<>(directoryOperations, ImmediateLogSynchronizer.factory());
    initialLog.initialize(() -> {
      throw new AssertionError("should not be called");
    });
    assertThat(directoryOperations.list()).isEmpty();

    var writer = initialLog.createWriter();
    writer.logPut(ByteArrayWrapper.of(1), ByteArrayWrapper.of(2));
    writer.logDelete(ByteArrayWrapper.of(3));
    writer.close();
    assertThat(directoryOperations.list()).hasSize(1);

    initialLog.freeWriter(writer.getName());
    assertThat(directoryOperations.list()).isEmpty();

    var secondLog = new WriteAheadLog<>(directoryOperations, ImmediateLogSynchronizer.factory());
    secondLog.initialize(() -> {
      throw new AssertionError("should not be called");
    });
  }

  @Test public void testRecover() throws Exception {
    var directoryOperations = new MemoryFileDirectoryOperations();
    var initialLog = new WriteAheadLog<>(directoryOperations, ImmediateLogSynchronizer.factory());
    initialLog.initialize(() -> {
      throw new AssertionError("should not be called");
    });

    var putKey = ByteArrayWrapper.of(1);
    var putValue = ByteArrayWrapper.of(2);
    var deleteKey = ByteArrayWrapper.of(1);

    var writer = initialLog.createWriter();
    writer.logPut(putKey, putValue);
    writer.logDelete(deleteKey);
    writer.close();

    var consumer = new LogConsumerCapture();
    var recoverLog = new WriteAheadLog<>(directoryOperations, ImmediateLogSynchronizer.factory());
    recoverLog.initialize(() -> consumer);

    assertThat(consumer.operations).isEqualTo(
        List.of(
            Pair.of("put", Pair.of(putKey, putValue)),
            Pair.of("delete", deleteKey),
            Pair.of("persist", null)));

    assertThat(directoryOperations.list()).isEmpty();

    var postRecoverLog = new WriteAheadLog<>(directoryOperations, ImmediateLogSynchronizer.factory());
    postRecoverLog.initialize(() -> {
      throw new AssertionError("should not be called");
    });
  }

  private static class LogConsumerCapture implements LogConsumer {

    private final List<Pair<String, Object>> operations = new ArrayList<>();

    @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {
      operations.add(Pair.of("put", Pair.of(key, value)));
    }

    @Override public void delete(@NotNull ByteArrayWrapper key) throws IOException {
      operations.add(Pair.of("delete", key));
    }

    @Override public void persist() throws IOException {
      operations.add(Pair.of("persist", null));
    }
  }
}

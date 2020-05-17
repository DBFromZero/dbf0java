package dbf0.disk_key_value.readwrite.log;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.io.IOUtil;
import dbf0.disk_key_value.io.FileDirectoryOperations;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class WriteAheadLog<T extends OutputStream> {

  private static final Logger LOGGER = Dbf0Util.getLogger(WriteAheadLog.class);

  private final FileDirectoryOperations<T> directoryOperations;
  private final LogSynchronizer.Factory<T> logSynchronizerFactory;

  private enum State {
    UNINITIALIZED,
    INITIALIZED,
    CORRUPTED
  }

  private State state = State.UNINITIALIZED;
  private int nextLogIndex = 0;

  public WriteAheadLog(FileDirectoryOperations<T> directoryOperations,
                       LogSynchronizer.Factory<T> logSynchronizerFactory) {
    this.directoryOperations = directoryOperations;
    this.logSynchronizerFactory = logSynchronizerFactory;
  }

  public void initialize(@NotNull Supplier<LogConsumer> logConsumerSupplier) throws IOException {
    Preconditions.checkState(state == State.UNINITIALIZED);
    if (!directoryOperations.exists()) {
      directoryOperations.mkdirs();
    } else {
      var contents = directoryOperations.list();
      if (!contents.isEmpty()) {
        try {
          loadLogs(contents, logConsumerSupplier.get());
        } catch (Exception e) {
          state = State.CORRUPTED;
          var msg = "Exception in loading write-ahead-log. Assuming corrupted.";
          LOGGER.log(Level.SEVERE, msg, e);
          throw new RuntimeException(msg, e);
        }
      }
    }
    Preconditions.checkState(directoryOperations.list().isEmpty());
    state = State.INITIALIZED;
  }

  @NotNull public WriteAheadLogWriter createWriter() throws IOException {
    Preconditions.checkState(state == State.INITIALIZED);
    var name = String.valueOf(nextLogIndex++);
    LOGGER.info("Creating write-ahead-log " + name);
    var fileOperations = directoryOperations.file(name);
    Preconditions.checkState(!fileOperations.exists());
    var stream = fileOperations.createAppendOutputStream();
    return new WriteAheadLogWriter(name, stream, logSynchronizerFactory.create(fileOperations, stream));
  }

  public void freeWriter(@NotNull String name) throws IOException {
    Preconditions.checkState(state == State.INITIALIZED);
    LOGGER.info("Freeing write-ahead-log " + name);
    var fileOperations = directoryOperations.file(name);
    fileOperations.delete();
  }

  private void loadLogs(Collection<String> unorderedLogs, LogConsumer logConsumer) throws IOException {
    LOGGER.warning("write-ahead-logs exist from previous run. loading: " + unorderedLogs);
    var orderedLogs = unorderedLogs.stream().sorted((a, b) -> Ints.compare(Integer.parseInt(a), Integer.parseInt(b)))
        .collect(Collectors.toList());
    for (String name : orderedLogs) {
      loadLog(name, logConsumer);
    }
  }

  private void loadLog(String name, LogConsumer logConsumer) throws IOException {
    LOGGER.info("loading write-ahead-log " + name);
    var fileOperations = directoryOperations.file(name);
    try (var stream = new BufferedInputStream(fileOperations.createInputStream())) {
      while (true) {
        var code = stream.read();
        if (code < 0) {
          break;
        }
        ByteArrayWrapper key;
        switch ((byte) code) {
          case WriteAheadLogConstants.PUT:
            key = IOUtil.readBytes(stream);
            var value = IOUtil.readBytes(stream);
            logConsumer.put(key, value);
            break;
          case WriteAheadLogConstants.DELETE:
            key = IOUtil.readBytes(stream);
            logConsumer.delete(key);
            break;
          default:
            var msg = "Unexpected control code " + code + " in log " + name;
            LOGGER.log(Level.SEVERE, msg);
            throw new RuntimeException(msg);
        }
      }
    }
    logConsumer.persist();
    LOGGER.info("finished loading and persisting write-ahead-log " + name + ". deleting");
    fileOperations.delete();
  }
}

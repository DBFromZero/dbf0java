package dbf0.disk_key_value.readwrite.log;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

public interface LogConsumer extends Closeable {

  void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException;

  void delete(@NotNull ByteArrayWrapper key) throws IOException;

  void persist() throws IOException;
}

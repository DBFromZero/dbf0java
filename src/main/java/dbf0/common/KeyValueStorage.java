package dbf0.common;

import java.io.IOException;
import java.util.Optional;

public interface KeyValueStorage {

  void initialize() throws IOException;

  void close() throws IOException;

  void store(ByteArrayWrapper key, ByteArrayWrapper value) throws IOException;

  Optional<ByteArrayWrapper> get(ByteArrayWrapper key) throws IOException;
}

package dbf0.common.io;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

public interface Deserializer<T> {

  @NotNull T deserialize(InputStream s) throws IOException;

  default void skipDeserialize(InputStream s) throws IOException {
    deserialize(s);
  }
}

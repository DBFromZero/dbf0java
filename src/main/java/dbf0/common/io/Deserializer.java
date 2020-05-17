package dbf0.common.io;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public interface Deserializer<T> {

  @NotNull T deserialize(InputStream s) throws IOException;

  @NotNull default T deserialize(ByteArrayWrapper w) throws IOException {
    return deserialize(new ByteArrayInputStream(w.getArray()));
  }

  default void skipDeserialize(InputStream s) throws IOException {
    deserialize(s);
  }
}

package dbf0.common.io;

import java.io.IOException;

public interface IoSupplier<T> {
  T get() throws IOException;
}

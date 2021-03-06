package dbf0.disk_key_value.io;

import java.io.IOException;
import java.io.InputStream;

public interface ReadOnlyFileOperations {

  boolean exists();

  long length();

  InputStream createInputStream() throws IOException;
}

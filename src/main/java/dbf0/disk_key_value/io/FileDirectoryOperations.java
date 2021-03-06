package dbf0.disk_key_value.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface FileDirectoryOperations<S extends OutputStream> {

  boolean exists();

  List<String> list() throws IOException;

  void mkdirs() throws IOException;

  void clear() throws IOException;

  FileOperations<S> file(String name);

  FileDirectoryOperations<S> subDirectory(String name);
}

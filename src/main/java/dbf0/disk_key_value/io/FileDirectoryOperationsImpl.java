package dbf0.disk_key_value.io;

import dbf0.disk_key_value.FileDirectoryOperations;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FileDirectoryOperationsImpl implements FileDirectoryOperations<FileOutputStream> {

  private final File file;

  public FileDirectoryOperationsImpl(File file) {
    this.file = file;
  }

  @Override public List<String> list() throws IOException {
    return Arrays.asList(file.list());
  }

  @Override public boolean exists() {
    return file.exists();
  }

  @Override public void mkdirs() throws IOException {
    var made = file.mkdirs();
    if (!made && !exists()) {
      throw new IOException("failed to create directory " + file);
    }
  }


  @Override public void clear() throws IOException {
    for (String s : list()) {
      var child = new File(file, s);
      if (!child.delete()) {
        throw new IOException("Failed to delete " + child);
      }
    }
  }

  @Override public FileOperations<FileOutputStream> file(String name) {
    return new FileOperationsImpl(new File(file, name), "-tmp");
  }
}

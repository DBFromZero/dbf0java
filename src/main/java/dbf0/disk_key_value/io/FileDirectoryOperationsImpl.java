package dbf0.disk_key_value.io;

import com.google.common.base.MoreObjects;

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
    clearDirectory(file);
  }

  private static void clearDirectory(File file) throws IOException {
    for (var child : file.listFiles()) {
      if (child.isDirectory()) {
        clearDirectory(child);
      }
      if (!child.delete()) {
        throw new IOException("Failed to delete " + child);
      }
    }
  }

  @Override public FileOperations<FileOutputStream> file(String name) {
    return new FileOperationsImpl(new File(file, name), "-tmp");
  }

  @Override public FileDirectoryOperationsImpl subDirectory(String name) {
    return new FileDirectoryOperationsImpl(new File(file, name));
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file)
        .add("exists", exists())
        .toString();
  }
}

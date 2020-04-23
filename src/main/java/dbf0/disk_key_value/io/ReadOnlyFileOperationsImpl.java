package dbf0.disk_key_value.io;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

public class ReadOnlyFileOperationsImpl implements ReadOnlyFileOperations {

  private static final Logger LOGGER = Dbf0Util.getLogger(ReadOnlyFileOperationsImpl.class);

  protected final File file;

  public ReadOnlyFileOperationsImpl(File file) {
    this.file = Preconditions.checkNotNull(file);
  }

  @Override public boolean exists() throws IOException {
    if (!file.exists()) {
      return false;
    }
    Preconditions.checkState(file.isFile(), "%s is not a file", file);
    return true;
  }

  @Override public InputStream createInputStream() throws IOException {
    return new FileInputStream(file);
  }
}

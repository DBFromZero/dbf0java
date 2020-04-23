package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileOperationsImpl implements FileOperations<FileOutputStream> {

  private static final Logger LOGGER = Dbf0Util.getLogger(FileOperationsImpl.class);

  private final File file;
  private final String tempSuffix;

  public FileOperationsImpl(File file, String tempSuffix) {
    this.file = Preconditions.checkNotNull(file);
    this.tempSuffix = Preconditions.checkNotNull(tempSuffix);
  }

  @Override public boolean exists() throws IOException {
    if (!file.exists()) {
      return false;
    }
    Preconditions.checkState(file.isFile(), "%s is not a file", file);
    return true;
  }

  @Override public FileOutputStream createAppendOutputStream() throws IOException {
    return new FileOutputStream(file, true);
  }

  @Override public void sync(FileOutputStream outputStream) throws IOException {
    outputStream.getFD().sync();
  }

  @Override public InputStream createInputStream() throws IOException {
    return new FileInputStream(file);
  }

  @Override public OverWriter<FileOutputStream> createOverWriter() throws IOException {
    var tempFile = new File(file.getPath() + "-vacuum");
    if (tempFile.exists()) {
      throw new IOException("temp path " + tempFile + " already exists");
    }
    return new FileOverWriter(tempFile, new FileOutputStream(tempFile));
  }

  private class FileOverWriter implements OverWriter<FileOutputStream> {

    private final File tempFile;
    private final FileOutputStream stream;
    private boolean inProgress = true;

    public FileOverWriter(File tempFile, FileOutputStream stream) {
      this.tempFile = tempFile;
      this.stream = stream;
    }

    @Override public FileOutputStream getOutputStream() {
      Preconditions.checkState(inProgress);
      return stream;
    }

    @Override public void commit() throws IOException {
      Preconditions.checkState(inProgress);
      LOGGER.fine("Committing writing " + file);
      inProgress = false;
      try {
        tryClose();
        var renamed = tempFile.renameTo(file);
        if (!renamed) {
          throw new IOException("Failed to rename vacuum file, " + tempFile);
        }
      } catch (IOException e) {
        abort();
        throw e;
      }
    }


    @Override public void abort() {
      Preconditions.checkState(inProgress);
      inProgress = false;
      LOGGER.info("Aborting writing " + file);
      try {
        tryClose();
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, e, () -> "Ignoring error in closing file");
      } finally {
        try {
          var deleted = tempFile.delete();
          if (deleted) {
            LOGGER.info("Deleted temporary path " + tempFile);
          } else {
            LOGGER.warning("Failed to deleted temporary path " + tempFile +
                ". Will have to be removed manually");
          }
        } catch (Exception e) {
          LOGGER.log(Level.SEVERE, e, () -> "Error in deleting temporary path");
        }
      }
    }

    private void tryClose() throws IOException {
      try {
        stream.close();
      } catch (IOException e) {
        if (!e.getMessage().equals("Stream Closed")) {
          throw e;
        }
      }
    }
  }
}

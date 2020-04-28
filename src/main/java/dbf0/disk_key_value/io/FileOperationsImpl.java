package dbf0.disk_key_value.io;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileOperationsImpl extends ReadOnlyFileOperationsImpl implements FileOperations<FileOutputStream> {

  private static final Logger LOGGER = Dbf0Util.getLogger(FileOperationsImpl.class);

  private final String tempSuffix;

  public FileOperationsImpl(File file, String tempSuffix) {
    super(file);
    this.tempSuffix = Preconditions.checkNotNull(tempSuffix);
  }

  @Override public void delete() throws IOException {
    var deleted = file.delete();
    if (!deleted) {
      throw new IOException("Failed to delete " + file);
    }
  }

  @Override public FileOutputStream createAppendOutputStream() throws IOException {
    return new FileOutputStream(file, true);
  }

  @Override public void sync(FileOutputStream outputStream) throws IOException {
    outputStream.getFD().sync();
  }

  @Override public OverWriter<FileOutputStream> createOverWriter() throws IOException {
    var tempFile = new File(file.getPath() + tempSuffix);
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
      if (!inProgress) {
        LOGGER.warning("Aborting while not in progress");
        return;
      }
      inProgress = false;
      LOGGER.info("Aborting writing " + file);
      try {
        tryClose();
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, e, () -> "Ignoring error in closing file");
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
          LOGGER.log(Level.WARNING, e, () -> "Error in deleting temporary path");
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

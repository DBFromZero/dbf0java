package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileOperations;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WriteJob<T extends OutputStream, W> implements Runnable {

  public interface SortAndWriter<T extends OutputStream, W> {
    void sortAndWrite(T dataStream, T indexStream, W writes) throws IOException;
  }

  private static final Logger LOGGER = Dbf0Util.getLogger(WriteJob.class);

  private final String name;
  private final boolean isBase;
  private final int delta;
  private final PendingWritesAndLog<W> pendingWritesAndLog;
  private final FileOperations<T> fileOperations;
  private final FileOperations<T> indexFileOperations;
  private final WriteJobCoordinator<T, W> coordinator;
  private final SortAndWriter<T, W> writer;

  public WriteJob(String name, boolean isBase, int delta,
                  PendingWritesAndLog<W> pendingWritesAndLog,
                  FileOperations<T> fileOperations,
                  FileOperations<T> indexFileOperations,
                  WriteJobCoordinator<T, W> coordinator,
                  SortAndWriter<T, W> writer) {
    this.name = name;
    this.isBase = isBase;
    this.delta = delta;
    this.pendingWritesAndLog = pendingWritesAndLog;
    this.fileOperations = fileOperations;
    this.indexFileOperations = indexFileOperations;
    this.coordinator = coordinator;
    this.writer = writer;
  }

  public String getName() {
    return name;
  }

  public boolean isBase() {
    return isBase;
  }

  public int getDelta() {
    Preconditions.checkState(!isBase);
    return delta;
  }

  public PendingWritesAndLog<W> getPendingWritesAndLog() {
    return pendingWritesAndLog;
  }

  @Override public void run() {
    FileOperations.OverWriter<T> overWriter = null, indexOverWriter = null;
    try {
      Preconditions.checkState(!fileOperations.exists());
      Preconditions.checkState(!indexFileOperations.exists());

      overWriter = fileOperations.createOverWriter();
      indexOverWriter = indexFileOperations.createOverWriter();

      writer.sortAndWrite(overWriter.getOutputStream(), indexOverWriter.getOutputStream(),
          pendingWritesAndLog.getWrites());

      overWriter.commit();
      indexOverWriter.commit();
      coordinator.commitWrites(this);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, e, () -> "error in writing sorted file for " + name);
      if (overWriter != null) {
        overWriter.abort();
      }
      if (indexOverWriter != null) {
        indexOverWriter.abort();
      }
      coordinator.abortWrites(this);
    }
  }
}

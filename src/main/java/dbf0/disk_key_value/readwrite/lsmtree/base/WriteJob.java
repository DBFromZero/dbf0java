package dbf0.disk_key_value.readwrite.lsmtree.base;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.io.PositionTrackingStream;
import dbf0.disk_key_value.io.FileOperations;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import static dbf0.disk_key_value.readwrite.lsmtree.base.BaseDeltaMergerCron.BUFFER_SIZE;

public class WriteJob<T extends OutputStream, W, P extends PendingWrites<W>> implements Runnable {

  public interface SortAndWriter<W> {
    void sortAndWrite(PositionTrackingStream dataStream, BufferedOutputStream indexStream, W writes, boolean isBase)
        throws IOException;
  }

  private static final Logger LOGGER = Dbf0Util.getLogger(WriteJob.class);

  private final String name;
  private final boolean isBase;
  private final int delta;
  private final P pendingWrites;
  private final FileOperations<T> fileOperations;
  private final FileOperations<T> indexFileOperations;
  private final WriteJobCoordinator<T, W, P> coordinator;
  private final SortAndWriter<W> writer;

  public WriteJob(String name, boolean isBase, int delta,
                  P pendingWrites,
                  FileOperations<T> fileOperations,
                  FileOperations<T> indexFileOperations,
                  WriteJobCoordinator<T, W, P> coordinator,
                  SortAndWriter<W> writer) {
    this.name = name;
    this.isBase = isBase;
    this.delta = delta;
    this.pendingWrites = pendingWrites;
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

  public P getPendingWrites() {
    return pendingWrites;
  }

  @Override public void run() {
    FileOperations.OverWriter<T> overWriter = null, indexOverWriter = null;
    try {
      Preconditions.checkState(!fileOperations.exists());
      Preconditions.checkState(!indexFileOperations.exists());

      overWriter = fileOperations.createOverWriter();
      indexOverWriter = indexFileOperations.createOverWriter();

      try (var outputStream = new PositionTrackingStream(overWriter.getOutputStream(), BUFFER_SIZE)) {
        try (var indexOutputStream = new BufferedOutputStream(indexOverWriter.getOutputStream(), BUFFER_SIZE)) {
          writer.sortAndWrite(outputStream, indexOutputStream, pendingWrites.getWrites(), isBase);
        }
      }

      overWriter.commit();
      indexOverWriter.commit();
      coordinator.commitWrites(this);
    } catch (Throwable t) {
      LOGGER.log(Level.SEVERE, t, () -> "error in writing sorted file for " + name);
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

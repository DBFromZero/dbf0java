package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.PositionTrackingStream;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.KeyValueFileWriter;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WriteSortedEntriesJob<T extends OutputStream> implements Runnable {

  private static final Logger LOGGER = Dbf0Util.getLogger(WriteSortedEntriesJob.class);
  private static final int IO_BUFFER_SIZE = 0x4000;

  private final String name;
  private final boolean isBase;
  private final int delta;
  private final int indexRate;
  private final Map<ByteArrayWrapper, ByteArrayWrapper> writes;
  private final FileOperations<T> fileOperations;
  private final FileOperations<T> indexFileOperations;
  private final DeltaWriterCoordinator<T> coordinator;
  private LsmBackgroundTaskState state = LsmBackgroundTaskState.PENDING;

  public WriteSortedEntriesJob(String name,
                               boolean isBase,
                               int delta,
                               int indexRate,
                               Map<ByteArrayWrapper, ByteArrayWrapper> writes,
                               FileOperations<T> fileOperations,
                               FileOperations<T> indexFileOperations,
                               DeltaWriterCoordinator<T> coordinator) {
    this.name = name;
    this.isBase = isBase;
    this.delta = delta;
    this.indexRate = indexRate;
    this.writes = writes;
    this.fileOperations = fileOperations;
    this.indexFileOperations = indexFileOperations;
    this.coordinator = coordinator;
  }

  public LsmBackgroundTaskState getState() {
    return state;
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

  public Map<ByteArrayWrapper, ByteArrayWrapper> getWrites() {
    return writes;
  }

  @Override public void run() {
    FileOperations.OverWriter<T> overWriter = null, indexOverWriter = null;
    try {
      Preconditions.checkState(state == LsmBackgroundTaskState.PENDING);
      Preconditions.checkState(!fileOperations.exists());

      LOGGER.info(() -> "Sorting " + writes.size() + " writes for " + name);
      var sortedEntries = new ArrayList<>(writes.entrySet());
      sortedEntries.sort(Map.Entry.comparingByKey());

      overWriter = fileOperations.createOverWriter();
      indexOverWriter = indexFileOperations.createOverWriter();

      try (var outputStream = new PositionTrackingStream(overWriter.getOutputStream(), IO_BUFFER_SIZE)) {
        try (var indexWriter = new KeyValueFileWriter(
            new BufferedOutputStream(indexOverWriter.getOutputStream(), IO_BUFFER_SIZE))) {
          var indexBuilder = IndexBuilder.indexBuilder(indexWriter, indexRate);
          try (var writer = new KeyValueFileWriter(outputStream)) {
            for (var entry : sortedEntries) {
              indexBuilder.accept(outputStream.getPosition(), entry.getKey());
              writer.append(entry.getKey(), entry.getValue());
            }
          }
        }
      }

      LOGGER.info(() -> "Committing " + name);
      state = LsmBackgroundTaskState.SUCCEEDED;
      overWriter.commit();
      indexOverWriter.commit();
      coordinator.commitWrites(this);

    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, e, () -> "error in writing sorted file for " + name);
      state = LsmBackgroundTaskState.FAILED;
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

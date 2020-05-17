package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.io.PositionTrackingStream;
import dbf0.common.io.Serializer;
import dbf0.common.io.UnsignedLongSerializer;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.readonly.IndexBuilder;
import dbf0.disk_key_value.readonly.KeyValueFileWriter;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WriteSortedEntriesJob<T extends OutputStream, K, V> implements Runnable {

  private static final Logger LOGGER = Dbf0Util.getLogger(WriteSortedEntriesJob.class);
  private static final int IO_BUFFER_SIZE = 0x4000;

  private final String name;
  private final boolean isBase;
  private final int delta;
  private final int indexRate;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final Comparator<K> keyComparator;
  private final PendingWritesAndLog<K, V> pendingWritesAndLog;
  private final FileOperations<T> fileOperations;
  private final FileOperations<T> indexFileOperations;
  private final DeltaWriterCoordinator<T, K, V> coordinator;

  public WriteSortedEntriesJob(String name, boolean isBase,
                               int delta, int indexRate,
                               Serializer<K> keySerializer,
                               Serializer<V> valueSerializer,
                               Comparator<K> keyComparator,
                               PendingWritesAndLog<K, V> pendingWritesAndLog,
                               FileOperations<T> fileOperations,
                               FileOperations<T> indexFileOperations,
                               DeltaWriterCoordinator<T, K, V> coordinator) {
    this.name = name;
    this.isBase = isBase;
    this.delta = delta;
    this.indexRate = indexRate;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyComparator = keyComparator;
    this.pendingWritesAndLog = pendingWritesAndLog;
    this.fileOperations = fileOperations;
    this.indexFileOperations = indexFileOperations;
    this.coordinator = coordinator;
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

  PendingWritesAndLog<K, V> getPendingWritesAndLog() {
    return pendingWritesAndLog;
  }

  @Override public void run() {
    FileOperations.OverWriter<T> overWriter = null, indexOverWriter = null;
    try {
      Preconditions.checkState(!fileOperations.exists());
      Preconditions.checkState(!indexFileOperations.exists());
      LOGGER.info(() -> "Sorting " + pendingWritesAndLog.writes.size() + " writes for " + name);
      var sortedEntries = new ArrayList<>(pendingWritesAndLog.writes.entrySet());
      sortedEntries.sort(Map.Entry.comparingByKey(keyComparator));

      overWriter = fileOperations.createOverWriter();
      indexOverWriter = indexFileOperations.createOverWriter();

      try (var outputStream = new PositionTrackingStream(overWriter.getOutputStream(), IO_BUFFER_SIZE)) {
        try (var indexWriter = new KeyValueFileWriter<>(keySerializer, UnsignedLongSerializer.getInstance(),
            new BufferedOutputStream(indexOverWriter.getOutputStream(), IO_BUFFER_SIZE))) {
          var indexBuilder = IndexBuilder.indexBuilder(indexWriter, indexRate);
          try (var writer = new KeyValueFileWriter<>(keySerializer, valueSerializer, outputStream)) {
            for (var entry : sortedEntries) {
              indexBuilder.accept(outputStream.getPosition(), entry.getKey());
              writer.append(entry.getKey(), entry.getValue());
            }
          }
        }
      }
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

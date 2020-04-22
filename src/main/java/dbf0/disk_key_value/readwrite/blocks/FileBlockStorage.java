package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Preconditions;
import dbf0.common.Dbf0Util;
import dbf0.common.PositionTrackingStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileBlockStorage implements BlockStorage {

  private final File file;
  private final MetadataStorage metadataStorage;

  private FileOutputStream fileOutputStream;
  private PositionTrackingStream stream;
  private SerializationHelper serializationHelper;
  private FileBlockWriter blockWriter;
  private boolean inBatch = false;
  private final Map<Long, Integer> usedBlockLengths = new HashMap<>();
  private final Map<Long, Integer> unusedBlocksLengths = new HashMap<>();


  public FileBlockStorage(File file, MetadataStorage metadataStorage) {
    this.file = file;
    this.metadataStorage = metadataStorage;
  }

  public void initialize() throws IOException {
    if (file.exists()) {
      Preconditions.checkState(file.isFile(), "%s is not a file", file);
    } else {
      createNew();
    }
  }

  @Override public void startBatchWrites() throws IOException {
    Preconditions.checkState(!inBatch);
    inBatch = true;
  }

  @Override public void endBatchWrites() throws IOException {
    Preconditions.checkState(inBatch);
    inBatch = false;
    postWrite();
  }

  private class FileBlockWriter extends BaseBlockWriter<SerializationHelper> {

    public FileBlockWriter(long blockId, SerializationHelper serializer) {
      super(blockId, serializer);
    }

    @Override public void commit() throws IOException {
      Preconditions.checkState(!isCommitted);
      Preconditions.checkState(blockWriter == this);
      isCommitted = true;
      //TODO: safe long to int
      var length = (int) (stream.getPosition() - blockId);
      Preconditions.checkState(length > 0, "nothing written");
      usedBlockLengths.put(blockId, length);
      blockWriter = null;
      if (!inBatch) {
        postWrite();
      }
    }
  }

  @Override public BlockWriter writeBlock() throws IOException {
    Preconditions.checkState(stream != null, " not initialized");
    Preconditions.checkState(blockWriter == null, "already writing");
    blockWriter = new FileBlockWriter(stream.getPosition(), serializationHelper);
    return blockWriter;
  }

  @Override public DeserializationHelper readBlock(long blockId) throws IOException {
    var length = usedBlockLengths.get(blockId);
    Preconditions.checkArgument(length != null, "no such block id %s", blockId);

    try (var inputStream = new FileInputStream(file)) {
      var skipped = inputStream.skip(blockId);
      Preconditions.checkState(skipped == blockId, "failed to skip %s, only skipped %",
          blockId, skipped);

      // TODO, if block is large enough, return a DeserializationHelper backed by a stream
      var bytes = new byte[length];
      Dbf0Util.readArrayFully(inputStream, bytes);
      return new DeserializationHelper(bytes);
    }
  }

  @Override public void freeBlock(long blockId) throws IOException {
    var length = usedBlockLengths.remove(blockId);
    Preconditions.checkArgument(length != null, "no such block id %s", blockId);
    unusedBlocksLengths.put(blockId, length);
    if (!inBatch) {
      postWrite();
    }
  }

  @Override public BlockStats getStats() {
    return new BlockStats(counts(usedBlockLengths), counts(unusedBlocksLengths));
  }

  @Override public <T> void vacuum(BlockReWriter<T> blockReWriter) throws IOException {
  }

  private void createNew() throws IOException {
    fileOutputStream = new FileOutputStream(file);
    stream = new PositionTrackingStream(fileOutputStream);
    serializationHelper = new SerializationHelper(stream);
  }

  private void load() throws IOException {
    throw new RuntimeException("not implemented");
  }

  private void writeMetadata() throws IOException {
    metadataStorage.updateMetadata(helper -> {
      helper.writeMap(usedBlockLengths, Serializer.longSerializer(), Serializer.intSerializer());
      helper.writeMap(unusedBlocksLengths, Serializer.longSerializer(), Serializer.intSerializer());
    });
  }

  private static BlockCounts counts(Map<Long, Integer> map) {
    return new BlockCounts(map.size(), map.values().stream().mapToLong(x -> x).sum());
  }

  private void postWrite() throws IOException {
    stream.flush();
    fileOutputStream.getFD().sync();
    writeMetadata();
  }
}

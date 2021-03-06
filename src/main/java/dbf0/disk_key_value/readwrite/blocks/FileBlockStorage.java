package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import dbf0.common.Dbf0Util;
import dbf0.common.io.IOUtil;
import dbf0.common.io.PositionTrackingStream;
import dbf0.disk_key_value.io.*;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class FileBlockStorage<T extends OutputStream> implements BlockStorage {

  private static final Logger LOGGER = Dbf0Util.getLogger(FileBlockStorage.class);

  private final FileOperations<T> fileOperations;
  private final MetadataMap<Long, Integer> usedBlockLengths;
  private final MetadataMap<Long, Integer> unusedBlocksLengths;

  private T outputStream;
  private PositionTrackingStream positionTrackingStream;
  private DeprecatedSerializationHelper serializationHelper;
  private FileBlockWriter blockWriter;
  private boolean inBatch = false;


  public FileBlockStorage(FileOperations<T> fileOperations,
                          MetadataMap<Long, Integer> usedBlockLengths,
                          MetadataMap<Long, Integer> unusedBlocksLengths) {
    this.fileOperations = fileOperations;
    this.usedBlockLengths = usedBlockLengths;
    this.unusedBlocksLengths = unusedBlocksLengths;
  }

  public static FileBlockStorage<FileOutputStream> forFile(File file, FileMetadataStorage<?> metadataStorage) {
    return new FileBlockStorage<>(new FileOperationsImpl(file, "-vacuum"),
        metadataStorage.newMap("used-blocks", DeprecatedSerializationHelper::writeLong, DeprecatedSerializationHelper::writeInt),
        metadataStorage.newMap("unused-blocks", DeprecatedSerializationHelper::writeLong, DeprecatedSerializationHelper::writeInt));
  }

  public static FileBlockStorage<MemoryFileOperations.MemoryOutputStream> inMemory() {
    return new FileBlockStorage<>(new MemoryFileOperations(), new MemoryMetadataMap<>(), new MemoryMetadataMap<>());
  }

  public void initialize() throws IOException {
    if (fileOperations.exists()) {
    } else {
      createNew();
    }
  }

  @Override public void close() throws IOException {
    Preconditions.checkState(positionTrackingStream != null, " not initialized");
    positionTrackingStream.close();
    positionTrackingStream = null;
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

  private class FileBlockWriter extends BaseBlockWriter<DeprecatedSerializationHelper> {

    public FileBlockWriter(long blockId, DeprecatedSerializationHelper serializer) {
      super(blockId, serializer);
    }

    @Override public void commit() throws IOException {
      Preconditions.checkState(!isCommitted);
      Preconditions.checkState(blockWriter == this);
      isCommitted = true;
      //TODO: safe long to int
      var length = (int) (positionTrackingStream.getPosition() - blockId);
      Preconditions.checkState(length > 0, "nothing written");
      usedBlockLengths.put(blockId, length);
      blockWriter = null;
      if (!inBatch) {
        postWrite();
      }
    }
  }

  @Override public BlockWriter writeBlock() throws IOException {
    Preconditions.checkState(positionTrackingStream != null, " not initialized");
    Preconditions.checkState(blockWriter == null, "already writing");
    blockWriter = new FileBlockWriter(positionTrackingStream.getPosition(), serializationHelper);
    return blockWriter;
  }

  @Override public DeserializationHelper readBlock(long blockId) throws IOException {
    var length = usedBlockLengths.get(blockId);
    if (length == null) {
      System.out.println();
    }
    Preconditions.checkArgument(length != null, "no such block id %s", blockId);

    try (var inputStream = fileOperations.createInputStream()) {
      skip(inputStream, blockId);

      // TODO, if block is large enough, return a DeserializationHelper backed by a stream
      var bytes = new byte[length];
      IOUtil.readArrayFully(inputStream, bytes);
      return new DeserializationHelper(bytes);
    }
  }

  @Override public void freeBlock(long blockId) throws IOException {
    var length = usedBlockLengths.delete(blockId);
    Preconditions.checkArgument(length != null, "no such block id %s", blockId);
    unusedBlocksLengths.put(blockId, length);
    if (!inBatch) {
      postWrite();
    }
  }

  @Override public BlockStats getStats() {
    return new BlockStats(counts(usedBlockLengths), counts(unusedBlocksLengths));
  }

  @Override public FileBlockStorageVacuum vacuum() {
    return new FileBlockStorageVacuum();
  }

  private void createNew() throws IOException {
    setupOutputStream(0L);
  }

  private void setupOutputStream(long startingPosition) throws IOException {
    outputStream = fileOperations.createAppendOutputStream();
    positionTrackingStream = new PositionTrackingStream(outputStream, PositionTrackingStream.DEFAULT_BUFFER_SIZE, startingPosition);
    serializationHelper = new DeprecatedSerializationHelper(positionTrackingStream);
  }

  private void load() throws IOException {
    throw new RuntimeException("not implemented");
  }

  private static BlockCounts counts(MetadataMap<Long, Integer> map) {
    var m = map.unmodifiableMap();
    return new BlockCounts(m.size(), m.values().stream().mapToLong(x -> x).sum());
  }

  private void postWrite() throws IOException {
    positionTrackingStream.flush();
    fileOperations.sync(outputStream);
  }

  private class FileBlockStorageVacuum implements BlockStorageVacuum {

    private FileOperations.OverWriter<T> overWriter;
    private Map<Long, Long> mapping;
    private long finalLength;

    @Override public void writeNewFile() throws IOException {
      Preconditions.checkState(!inBatch);
      Preconditions.checkState(overWriter == null);
      LOGGER.info(() -> "Vacuuming. Initial stats: " + getStats());
      overWriter = fileOperations.createOverWriter();
      try (var vacuumStream = new PositionTrackingStream(overWriter.getOutputStream())) {
        mapping = runVacuum(vacuumStream);
        finalLength = vacuumStream.getPosition();
      }
    }

    @Override public Map<Long, Long> commit() throws IOException {
      try {
        Preconditions.checkState(!inBatch);
        Preconditions.checkState(overWriter != null);
        Preconditions.checkState(mapping != null);
        overWriter.commit();
        updateBlockMappings(mapping);
        setupOutputStream(finalLength);
        LOGGER.info(() -> "Vacuuming succeeded. Final stats: " + getStats());
        return mapping;
      } catch (IOException e) {
        LOGGER.warning(() -> Strings.lenientFormat("Vacuuming failed due to %s. Attempting to abort", e));
        abort();
        throw e;
      }
    }

    @Override public void abort() {
      overWriter.abort();
    }
  }

  private Map<Long, Long> runVacuum(PositionTrackingStream vacuumStream) throws IOException {
    Map<Long, Long> oldToNewBlockOffsets = new HashMap<>();
    var sortedBlockOffsets = usedBlockLengths.unmodifiableMap().keySet().stream().sorted().collect(Collectors.toList());
    long currentInputPosition = 0;
    var buffer = new byte[2048];
    try (var inputStream = fileOperations.createInputStream()) {
      for (var oldBlockOffset : sortedBlockOffsets) {
        var length = usedBlockLengths.get(oldBlockOffset);
        var newBlockOffset = vacuumStream.getPosition();
        LOGGER.fine(() -> "offset mapping old=" + oldBlockOffset + " -> " + newBlockOffset + " (len=" + length + ")");
        oldToNewBlockOffsets.put(oldBlockOffset, newBlockOffset);
        skip(inputStream, oldBlockOffset - currentInputPosition);
        copyBlock(inputStream, vacuumStream, length, buffer);
        currentInputPosition = oldBlockOffset + length;
      }
    }
    return oldToNewBlockOffsets;
  }

  private static void copyBlock(InputStream input, OutputStream output, int count, byte[] buffer) throws IOException {
    Preconditions.checkState(count > 0);
    while (count > 0) {
      var read = input.read(buffer, 0, Math.min(count, buffer.length));
      if (read <= 0) {
        throw new IOException("Unexpected end of stream w/ " + count + " remaining");
      }
      output.write(buffer, 0, read);
      count -= read;
    }
  }

  // only called after we've written the new file to update block length mappings
  private void updateBlockMappings(Map<Long, Long> oldToNewBlockOffsets) throws IOException {
    LOGGER.finer(() -> "Old block length: " + usedBlockLengths.toString());
    Preconditions.checkState(oldToNewBlockOffsets.size() == usedBlockLengths.size());
    var newUsedBlocks = oldToNewBlockOffsets.entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getValue,
        Functions.compose(usedBlockLengths::get, Map.Entry::getKey)));
    usedBlockLengths.clear();
    usedBlockLengths.putAll(newUsedBlocks);
    unusedBlocksLengths.clear();
    LOGGER.finer(() -> "New block length: " + usedBlockLengths.toString());
  }

  private static void skip(InputStream inputStream, long offset) throws IOException {
    var skipped = inputStream.skip(offset);
    Preconditions.checkState(skipped == offset, "failed to skip %s, only skipped %",
        offset, skipped);
  }
}

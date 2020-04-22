package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;

import java.io.IOException;

public class MemoryMetadataStorage implements MetadataStorage {

  private ByteArrayWrapper serializedMetadata;

  public MemoryMetadataStorage() {
  }

  public MemoryMetadataStorage(ByteArrayWrapper serializedMetadata) {
    this.serializedMetadata = serializedMetadata;
  }

  @Override public DeserializationHelper readMetadata() throws IOException {
    Preconditions.checkState(serializedMetadata != null);
    return new DeserializationHelper(serializedMetadata);
  }

  @Override public void updateMetadata(MetadataUpdater updater) throws IOException {
    var serializer = new ByteSerializationHelper();
    updater.update(serializer);
    serializedMetadata = serializer.getBytes();
  }
}

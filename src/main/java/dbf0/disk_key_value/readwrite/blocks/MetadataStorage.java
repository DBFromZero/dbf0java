package dbf0.disk_key_value.readwrite.blocks;

import java.io.IOException;

public interface MetadataStorage {
  DeserializationHelper readMetadata() throws IOException;

  void updateMetadata(MetadataUpdater updater) throws IOException;

  interface MetadataUpdater {
    void update(SerializationHelper helper) throws IOException;
  }
}

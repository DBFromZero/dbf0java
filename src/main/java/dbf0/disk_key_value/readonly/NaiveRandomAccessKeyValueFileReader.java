package dbf0.disk_key_value.readonly;

import dbf0.common.ByteArrayWrapper;

import javax.annotation.Nullable;
import java.io.IOException;

class NaiveRandomAccessKeyValueFileReader {

  private final String path;

  NaiveRandomAccessKeyValueFileReader(String path) {
    this.path = path;
  }

  @Nullable
  ByteArrayWrapper get(ByteArrayWrapper key) throws IOException {
    try (var reader = new KeyValueFileReader(path)) {
      while (true) {
        var entryKey = reader.readKey();
        if (entryKey == null) {
          return null;
        }
        if (entryKey.equals(key)) {
          return reader.readValue();
        } else {
          reader.skipValue();
        }
      }
    }
  }
}

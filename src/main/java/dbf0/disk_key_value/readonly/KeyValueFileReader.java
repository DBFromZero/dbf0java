package dbf0.disk_key_value.readonly;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.EndOfStream;
import dbf0.common.PrefixIo;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;

class KeyValueFileReader {

  private final FileInputStream stream;
  private boolean haveReadKey = false;

  KeyValueFileReader(FileInputStream stream) {
    this.stream = stream;
  }

  @Nullable
  ByteArrayWrapper readKey() throws IOException {
    Preconditions.checkState(!haveReadKey);
    ByteArrayWrapper key;
    try {
      key = PrefixIo.readBytes(stream);
    } catch (EndOfStream ignored) {
      return null;
    }
    haveReadKey = true;
    return key;
  }

  ByteArrayWrapper readValue() throws IOException {
    Preconditions.checkState(haveReadKey);
    var value = PrefixIo.readBytes(stream);
    haveReadKey = false;
    return value;
  }

  void skipValue() throws IOException {
    Preconditions.checkState(haveReadKey);
    int length = PrefixIo.readLength(stream);
    skipBytes(length);
    haveReadKey = false;
  }

  @Nullable
  Pair<ByteArrayWrapper, ByteArrayWrapper> readKeyValue() throws IOException {
    var key = readKey();
    if (key == null) {
      return null;
    }
    return Pair.of(key, readValue());
  }

  void skipBytes(long bytes) throws IOException {
    long skipped = stream.skip(bytes);
    if (skipped != bytes) {
      throw new RuntimeException("Failed to skip " + bytes + " only skipped" + skipped);
    }
  }
}

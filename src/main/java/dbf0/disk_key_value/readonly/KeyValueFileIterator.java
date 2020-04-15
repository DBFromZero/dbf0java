package dbf0.disk_key_value.readonly;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.EndOfStream;
import dbf0.common.PrefixIo;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

class KeyValueFileIterator implements Iterator<Pair<ByteArrayWrapper, ByteArrayWrapper>> {
  private final FileInputStream stream;
  private boolean hasReadNext = false;
  private Pair<ByteArrayWrapper, ByteArrayWrapper> next = null;

  public KeyValueFileIterator(FileInputStream stream) {
    this.stream = stream;
  }

  @Override
  public boolean hasNext() {
    if (!hasReadNext) {
      next = readNext();
      hasReadNext = next != null;
    }
    return hasReadNext;
  }

  @Override
  public Pair<ByteArrayWrapper, ByteArrayWrapper> next() {
    if (!hasNext()) {
      throw new RuntimeException("no next");
    }
    hasReadNext = false;
    var n = next;
    next = null;
    return n;
  }

  @Nullable
  private Pair<ByteArrayWrapper, ByteArrayWrapper> readNext() {
    try {
      ByteArrayWrapper key;
      try {
        key = PrefixIo.readBytes(stream);
      } catch (EndOfStream ignored) {
        return null;
      }
      ByteArrayWrapper value = PrefixIo.readBytes(stream);
      return Pair.of(key, value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

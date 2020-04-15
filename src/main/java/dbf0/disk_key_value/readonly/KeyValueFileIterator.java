package dbf0.disk_key_value.readonly;

import dbf0.common.ByteArrayWrapper;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

class KeyValueFileIterator implements Iterator<Pair<ByteArrayWrapper, ByteArrayWrapper>> {
  private final KeyValueFileReader reader;
  private boolean hasReadNext = false;
  private Pair<ByteArrayWrapper, ByteArrayWrapper> next = null;

  KeyValueFileIterator(KeyValueFileReader reader) {
    this.reader = reader;
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
      return reader.readKeyValue();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

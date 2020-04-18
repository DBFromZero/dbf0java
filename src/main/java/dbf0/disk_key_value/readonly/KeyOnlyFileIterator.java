package dbf0.disk_key_value.readonly;

import dbf0.common.ByteArrayWrapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

class KeyOnlyFileIterator implements Iterator<ByteArrayWrapper> {
  private final KeyValueFileReader reader;
  private boolean hasReadNext = false;
  private ByteArrayWrapper next = null;

  public KeyOnlyFileIterator(KeyValueFileReader reader) {
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
  public ByteArrayWrapper next() {
    if (!hasNext()) {
      throw new RuntimeException("no next");
    }
    hasReadNext = false;
    var n = next;
    next = null;
    return n;
  }

  @Nullable
  private ByteArrayWrapper readNext() {
    try {
      var key = reader.readKey();
      if (key != null) {
        reader.skipValue();
      }
      return key;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

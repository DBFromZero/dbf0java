package dbf0.disk_key_value.readonly.singlevalue;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

class KeyOnlyFileIterator<K> implements Iterator<K> {
  private final KeyValueFileReader<K, ?> reader;
  private boolean hasReadNext = false;
  private K next = null;

  public KeyOnlyFileIterator(KeyValueFileReader<K, ?> reader) {
    this.reader = Preconditions.checkNotNull(reader);
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
  public K next() {
    if (!hasNext()) {
      throw new RuntimeException("no next");
    }
    hasReadNext = false;
    var n = next;
    next = null;
    return n;
  }

  @Nullable
  private K readNext() {
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

package dbf0.disk_key_value;

import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.EndOfStream;
import dbf0.common.PrefixIo;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

class KeyFileIterator implements Iterator<ByteArrayWrapper> {
  private final FileInputStream stream;
  private boolean hasReadNext = false;
  private ByteArrayWrapper next = null;

  public KeyFileIterator(FileInputStream stream) {
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
      int totalLength;
      try {
        totalLength = PrefixIo.readLength(stream);
      } catch (EndOfStream ignored) {
        return null;
      }
      var key = PrefixIo.readBytes(stream);
      int valueLength = totalLength - key.length();
      long skipped = stream.skip(valueLength);
      Preconditions.checkState(skipped == valueLength);
      return key;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

package dbf0.disk_key_value.readonly.singlevalue;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

public class KeyValueFileIterator<K, V> implements Iterator<Pair<K, V>> {
  private final KeyValueFileReader<K, V> reader;
  private boolean hasReadNext = false;
  private Pair<K, V> next = null;

  public KeyValueFileIterator(KeyValueFileReader<K, V> reader) {
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
  public Pair<K, V> next() {
    if (!hasNext()) {
      throw new RuntimeException("no next");
    }
    hasReadNext = false;
    var n = next;
    next = null;
    return n;
  }

  @Nullable
  private Pair<K, V> readNext() {
    try {
      return reader.readKeyValue();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
